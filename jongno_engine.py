import osmnx as ox
from sqlalchemy import create_engine, text

# 1. DB 연결 설정: PostGIS가 설치된 PostgreSQL 서버에 접속
engine = create_engine("postgresql://macbook@localhost:5433/walk_service")

def enrich_everything():
    """
    종로구 내의 다양한 지리적 요소(안전, 녹지, 문화 등)를 추출하여 
    H3 격자별로 점수를 매기고 최종 가중치를 합산하는 함수
    """
    loc = "Jongno-gu, Seoul, South Korea"
    
    # 2. 분석 레이어 정의: 각 점수 항목과 매핑되는 OSM 태그 및 데이터 유형(Point/Polygon)
    layers = [
        {'col': 'safety_score', 'type': 'point', 'tags': {'highway': 'street_lamp', 'amenity': 'police'}},
        {'col': 'green_score', 'type': 'polygon', 'tags': {'leisure': 'park', 'landuse': 'grass'}},
        {'col': 'culture_score', 'type': 'polygon', 'tags': {'historic': True, 'amenity': 'museum'}},
        {'col': 'convenience_score', 'type': 'point', 'tags': {'amenity': 'toilets', 'shop': 'convenience'}},
        {'col': 'medical_score', 'type': 'point', 'tags': {'amenity': ['hospital', 'pharmacy']}},
        {'col': 'water_score', 'type': 'polygon', 'tags': {'waterway': True, 'natural': 'water'}},
        {'col': 'vitality_score', 'type': 'polygon', 'tags': {'landuse': 'commercial'}},
        {'col': 'pet_score', 'type': 'point', 'tags': {'leisure': 'dog_park'}}
    ]

    with engine.connect() as conn:
        for l in layers:
            print(f"📡 {l['col']} 데이터 수집 및 분석 중...")
            try:
                # 3. OSM 데이터 추출: 지정된 위치에서 반경 3km 내의 특정 태그 시설물을 가져옴
                feat = ox.features_from_address(loc, tags=l['tags'], dist=3000)
                if feat.empty: continue
                
                # 4. 임시 테이블(Temp Table) 생성: 공간 연산 속도 향상을 위해 수집한 데이터를 DB에 임시 저장
                conn.execute(text(f"DROP TABLE IF EXISTS t_{l['col']};"))
                conn.execute(text(f"CREATE TABLE t_{l['col']} (geom GEOMETRY);"))
                for g in feat.geometry:
                    # Shapely 도형 객체를 WKT로 변환하여 PostGIS geometry 타입으로 삽입
                    conn.execute(text(f"INSERT INTO t_{l['col']} VALUES (ST_GeomFromText('{g.wkt}', 4326));"))
                
                # 5. 공간 연산 로직 (Spatial Join & Scoring)
                if l['type'] == 'point':
                    # [점 데이터 로직] 격자(Hexagon) 내부에 포함된 시설물의 개수를 세어 점수 부여
                    # 가중치 0.2는 시설물 하나당 0.2점씩 누적되는 예시 수치
                    sql = f"""
                        UPDATE basemap_h3 b 
                        SET {l['col']} = sub.c * 0.2 
                        FROM (
                            SELECT b.hex_id, COUNT(*) as c 
                            FROM basemap_h3 b, t_{l['col']} t 
                            WHERE ST_Contains(b.geometry::geometry, t.geom) 
                            GROUP BY b.hex_id
                        ) AS sub 
                        WHERE b.hex_id = sub.hex_id
                    """
                else:
                    # [면 데이터 로직] 격자와 시설물이 겹치는 면적 비율(Intersection/Total Area) 계산
                    # 예: 격자의 50%가 공원이면 0.5점을 부여하는 방식
                    sql = f"""
                        UPDATE basemap_h3 b 
                        SET {l['col']} = sub.s 
                        FROM (
                            SELECT b.hex_id, 
                                   SUM(ST_Area(ST_Intersection(b.geometry::geometry, t.geom)) / ST_Area(b.geometry::geometry)) as s 
                            FROM basemap_h3 b, t_{l['col']} t 
                            WHERE ST_Intersects(b.geometry::geometry, t.geom) 
                            GROUP BY b.hex_id
                        ) AS sub 
                        WHERE b.hex_id = sub.hex_id
                    """
                
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(f"❌ {l['col']} 처리 중 오류 발생: {e}")
                continue

        # 6. 최종 통합 가중치 공식 (Weighted Sum): 각 지표의 중요도에 따라 최종 산책 지수(total_weight) 계산
        print("⚖️ 최종 가중치 합산 중...")
        conn.execute(text("""
            UPDATE basemap_h3 SET total_weight = 
            (COALESCE(safety_score, 0) * 0.3) + 
            (COALESCE(green_score, 0) * 0.2) + 
            (COALESCE(culture_score, 0) * 0.2) + 
            (COALESCE(convenience_score, 0) * 0.1) + 
            (COALESCE(medical_score, 0) * 0.2)
        """))
        conn.commit()
        print("🏁 종로구 지능형 지도 완성!")

if __name__ == "__main__":
    enrich_everything()