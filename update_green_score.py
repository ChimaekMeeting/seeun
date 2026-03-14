import osmnx as ox
from sqlalchemy import create_engine, text
import pandas as pd

# 1. DB 연결 설정 (아까와 동일한 포트 5433)
DB_URL = "postgresql://macbook@localhost:5433/walk_service" # whoami 결과로 수정하세요!
engine = create_engine(DB_URL)

def update_green_scores():
    print("🌿 강남역 주변 녹지(공원, 정원 등) 데이터를 수집 중...")
    location_name = "Gangnam Station, Seoul, South Korea"
    
    # OSM에서 녹지 정보만 필터링해서 가져오기
    tags = {'leisure': ['park', 'garden', 'nature_reserve'], 'landuse': ['grass', 'forest']}
    try:
        green_spaces = ox.features_from_address(location_name, tags=tags, dist=700)
    except:
        print("📍 주변에 검색된 녹지가 없습니다. 기본 점수로 진행합니다.")
        return

    print(f"📊 찾은 녹지 구역 수: {len(green_spaces)}개")

    # 2. 임시 테이블에 녹지 데이터 저장
    # 녹지 도형들을 WKT 텍스트로 변환
    green_spaces['wkt'] = green_spaces['geometry'].apply(lambda x: x.wkt)
    green_data = green_spaces[['wkt']].copy()
    
    with engine.connect() as conn:
        # 기존 임시 테이블 삭제 및 생성
        conn.execute(text("DROP TABLE IF EXISTS temp_green_spaces;"))
        conn.execute(text("CREATE TABLE temp_green_spaces (geom GEOMETRY(Geometry, 4326));"))
        
        # 녹지 데이터 삽입
        for wkt in green_data['wkt']:
            conn.execute(text("INSERT INTO temp_green_spaces (geom) VALUES (ST_GeomFromText(:wkt, 4326));"), {"wkt": wkt})
        
        print("⬢ 육각형과 녹지 구역 교차 연산 중 (Spatial Join)...")
        
        # 3. 육각형과 녹지가 겹치는 정도에 따라 점수 업데이트 (0.0 ~ 1.0)
        # 육각형 면적 중 녹지가 차지하는 비율을 계산하여 green_score에 저장합니다.
        update_query = text("""
            UPDATE basemap_h3 b
            SET green_score = sub.score
            FROM (
                SELECT b.hex_id, 
                       COALESCE(
                           SUM(ST_Area(ST_Intersection(b.geometry::geometry, g.geom::geometry)) / 
                           NULLIF(ST_Area(b.geometry::geometry), 0)), 0
                       ) as score
                FROM basemap_h3 b, temp_green_spaces g
                WHERE ST_Intersects(b.geometry::geometry, g.geom::geometry)
                GROUP BY b.hex_id
            ) AS sub
            WHERE b.hex_id = sub.hex_id;
        """)
        
        conn.execute(update_query)
        conn.commit()
        print("✅ 모든 육각형의 녹지 지수 업데이트 완료!")

if __name__ == "__main__":
    update_green_scores()