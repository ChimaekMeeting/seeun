import h3
import osmnx as ox
from sqlalchemy import create_engine, text
from shapely.geometry import Polygon

# 1. DB 연결 설정: PostgreSQL(PostGIS 확장) 엔진 생성
# 5433 포트와 walk_service DB를 사용하여 공간 데이터 처리를 준비합니다.
engine = create_engine("postgresql://macbook@localhost:5433/walk_service")

def create_jongno_grid():
    """
    종로구의 행정 구역 경계를 추출하여 H3 육각형 격자(Resolution 12)로 변환하고
    DB의 basemap_h3 테이블에 저장하는 메인 함수
    """
    print("📍 종로구 경계 추출 중...")
    # OSM(OpenStreetMap)에서 종로구의 경계면(Polygon) 데이터를 GeoDataFrame으로 가져옴
    gdf = ox.geocode_to_gdf("Jongno-gu, Seoul, South Korea")
    boundary = gdf.geometry.iloc[0] # 첫 번째 결과의 도형 정보 선택
    
    # H3 라이브러리 규격(위도, 경도)에 맞게 좌표 순서 변경 (Shapely는 경도, 위도 순)
    outer_coords = [(lat, lng) for lng, lat in boundary.exterior.coords]
    h3_poly = h3.LatLngPoly(outer_coords) # H3 내부 연산을 위한 다각형 객체 생성
    
    # 해상도 12 설정: 한 변의 길이가 약 10m 내외인 아주 세밀한 격자
    print("⬢ 종로구 육각형 격자 생성 중 (Res 12)...")
    hexs = list(h3.polygon_to_cells(h3_poly, 12)) 
    print(f"📊 생성된 육각형 수: {len(hexs)}개")

    with engine.connect() as conn:
        print("🧹 기존 데이터 정리 중...")
        # 멱등성(Idempotency) 보장을 위해 기존 데이터를 삭제하고 새로 주입
        conn.execute(text("DELETE FROM basemap_h3;"))
        conn.commit()

        print("📥 DB에 대량 주입 시작 (Batch Processing)...")
        # 11만 개가 넘는 데이터를 한 번에 넣으면 DB 부하가 크므로 1000개씩 끊어서 처리
        batch_size = 1000
        for i in range(0, len(hexs), batch_size):
            batch = hexs[i:i+batch_size]
            data_list = []
            
            for h in batch:
                # H3 인덱스(ID)로부터 육각형의 6개 꼭짓점 좌표를 추출
                coords = h3.cell_to_boundary(h)
                # DB 저장을 위해 '위도,경도'를 '경도,위도' 순의 WKT(Well-Known Text) 포맷으로 변환
                poly_wkt = Polygon([(lng, lat) for lat, lng in coords]).wkt
                data_list.append({"id": h, "wkt": poly_wkt})
            
            # PostGIS의 ST_GeomFromText를 사용하여 문자열을 실제 공간 기하 객체(SRID 4326)로 변환
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
