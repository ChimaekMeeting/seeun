import h3
import osmnx as ox
from sqlalchemy import create_engine, text
from shapely.geometry import Polygon

engine = create_engine("postgresql://macbook@localhost:5433/walk_service")

def create_jongno_grid():
    print("📍 종로구 경계 추출 중...")
    gdf = ox.geocode_to_gdf("Jongno-gu, Seoul, South Korea")
    boundary = gdf.geometry.iloc[0]
    
    outer_coords = [(lat, lng) for lng, lat in boundary.exterior.coords]
    h3_poly = h3.LatLngPoly(outer_coords)
    
    print("⬢ 종로구 육각형 격자 생성 중 (Res 12)...")
    hexs = list(h3.polygon_to_cells(h3_poly, 12)) # 리스트로 변환
    print(f"📊 생성된 육각형 수: {len(hexs)}개")

    with engine.connect() as conn:
        print("🧹 기존 데이터 정리 중...")
        conn.execute(text("DELETE FROM basemap_h3;"))
        conn.commit()

        print("📥 DB에 대량 주입 시작 (잠시만 기다려주세요)...")
        # 11만 개를 1000개씩 묶어서 배치 처리 (속도 향상)
        batch_size = 1000
        for i in range(0, len(hexs), batch_size):
            batch = hexs[i:i+batch_size]
            data_list = []
            for h in batch:
                coords = h3.cell_to_boundary(h)
                poly_wkt = Polygon([(lng, lat) for lat, lng in coords]).wkt
                data_list.append({"id": h, "wkt": poly_wkt})
            
            conn.execute(text("""
                INSERT INTO basemap_h3 (hex_id, geometry) 
                VALUES (:id, ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (hex_id) DO NOTHING
            """), data_list)
            
            if i % 10000 == 0:
                print(f" 진행률: {i}/{len(hexs)} 완료...")
        
        conn.commit()
    print("✅ 종로구 그리드 재생성 및 주입 완료!")

if __name__ == "__main__":
    create_jongno_grid()