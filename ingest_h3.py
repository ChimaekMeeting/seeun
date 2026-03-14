import osmnx as ox
import h3
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely.geometry import Polygon

# 1. DB 연결 설정 (Postgres.app 포트 5433)
# 사용자 이름(user)은 터미널에서 'whoami'를 입력했을 때 나오는 이름을 쓰세요.
DB_URL = "postgresql://macbook@localhost:5433/walk_service"
engine = create_engine(DB_URL)

def ingest_gangnam_h3():
    print("📡 강남역 주변 도로 데이터를 가져오는 중...")
    location_name = "Gangnam Station, Seoul, South Korea"
    # 강남역 중심 500m 이내 보행자 도로 데이터
    graph = ox.graph_from_address(location_name, dist=500, network_type='walk')
    
    # 2. 도로 노드(교차점) 추출
    nodes, _ = ox.graph_to_gdfs(graph)
    
    print("⬢ 육각형(H3) 변환 시작...")
    hexagons = {}
    for _, row in nodes.iterrows():
        # H3 Res 12 (약 9.4m 크기)로 변환
        hex_id = h3.latlng_to_cell(row['y'], row['x'], 12)
        
        if hex_id not in hexagons:
            # 육각형의 꼭짓점 좌표 가져오기
            boundary = h3.cell_to_boundary(hex_id)
            # Shapely Polygon으로 변환 (Lon, Lat 순서)
            poly = Polygon([(lon, lat) for lat, lon in boundary])
            hexagons[hex_id] = poly

    # 3. 데이터프레임 생성
    df = pd.DataFrame([
        {'hex_id': hid, 'geometry': geom.wkt, 'road_type': 'walkway'} 
        for hid, geom in hexagons.items()
    ])

    print(f"💾 {len(df)}개의 육각형 데이터를 DB에 저장하는 중...")
    
    # 4. SQL 실행 (Geom데이터는 WKT 포맷으로 전송 후 PostGIS에서 변환)
    df.to_sql('basemap_h3', engine, if_exists='append', index=False, method='multi')
    
    # PostGIS에서 텍스트를 실제 도형으로 변환하는 쿼리 실행
    with engine.connect() as conn:
        # text() 함수로 SQL을 감싸줍니다.
        sql = text("UPDATE basemap_h3 SET geometry = ST_GeomFromText(geometry, 4326) WHERE ST_GeometryType(geometry) IS NULL;")
        conn.execute(sql)
        # SQLAlchemy 2.0에서는 명시적으로 commit을 해줘야 반영됩니다.
        conn.commit()
        
    print("✅ 성공! 강남역 육각형 지도가 완성되었습니다.")

if __name__ == "__main__":
    ingest_gangnam_h3()