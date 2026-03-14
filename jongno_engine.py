import osmnx as ox
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://macbook@localhost:5433/walk_service")

def enrich_everything():
    loc = "Jongno-gu, Seoul, South Korea"
    
    # 데이터셋 2-2 ~ 2-17과 매핑되는 osm 태그
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
            print(f"📡 {l['col']} 데이터 수집 중...")
            try:
                feat = ox.features_from_address(loc, tags=l['tags'], dist=3000)
                if feat.empty: continue
                
                # 임시 테이블 활용 공간 연산
                conn.execute(text(f"DROP TABLE IF EXISTS t_{l['col']};"))
                conn.execute(text(f"CREATE TABLE t_{l['col']} (geom GEOMETRY);"))
                for g in feat.geometry:
                    conn.execute(text(f"INSERT INTO t_{l['col']} VALUES (ST_GeomFromText('{g.wkt}', 4326));"))
                
                if l['type'] == 'point':
                    sql = f"UPDATE basemap_h3 b SET {l['col']} = sub.c*0.2 FROM (SELECT b.hex_id, COUNT(*) as c FROM basemap_h3 b, t_{l['col']} t WHERE ST_Contains(b.geometry::geometry, t.geom) GROUP BY b.hex_id) AS sub WHERE b.hex_id = sub.hex_id"
                else:
                    sql = f"UPDATE basemap_h3 b SET {l['col']} = sub.s FROM (SELECT b.hex_id, SUM(ST_Area(ST_Intersection(b.geometry::geometry, t.geom))/ST_Area(b.geometry::geometry)) as s FROM basemap_h3 b, t_{l['col']} t WHERE ST_Intersects(b.geometry::geometry, t.geom) GROUP BY b.hex_id) AS sub WHERE b.hex_id = sub.hex_id"
                
                conn.execute(text(sql)); conn.commit()
            except: continue

        # 최종 통합 가중치 공식 적용
        print("⚖️ 최종 가중치 계산 중...")
        conn.execute(text("""
            UPDATE basemap_h3 SET total_weight = 
            (safety_score * 0.3) + (green_score * 0.2) + (culture_score * 0.2) + (convenience_score * 0.1) + (medical_score * 0.2)
        """)); conn.commit()
        print("🏁 종로구 지능형 지도 완성!")

if __name__ == "__main__":
    enrich_everything()