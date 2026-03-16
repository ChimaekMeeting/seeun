import folium
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely import wkb

# 1. DB 연결
engine = create_engine("postgresql://macbook@localhost:5433/walk_service")

def visualize():
    print("📍 종로구 가중치 데이터를 불러오는 중...")
    query = "SELECT hex_id, total_weight, ST_AsBinary(geometry) as geom FROM basemap_h3 WHERE total_weight > 0"
    df = pd.read_sql(query, engine)
    
    # 1. geometry 복원
    df['geometry'] = df['geom'].apply(lambda x: wkb.loads(bytes(x)))
    
    df.drop(columns=['geom'], inplace=True) 
    
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

    # 2. 지도 초기화 (종로구 중심)
    m = folium.Map(location=[37.573, 126.979], zoom_start=14, tiles='cartodbpositron')
    # 3. 단계 구분도(Choropleth) 추가
    # 가중치가 높을수록 'YlGn(노랑-초록)' 진해지도록 설정
    folium.Choropleth(
        geo_data=gdf.to_json(),
        name='Total Weight',
        data=gdf,
        columns=['hex_id', 'total_weight'],
        key_on='feature.properties.hex_id',
        fill_color='OrRd', 
        fill_opacity=0.9, 
        line_opacity=0.5, # 육각형 테두리를 진하게
        legend_name='보행 쾌적도 가중치'
    ).add_to(m)

    # 4. 결과 저장
    m.save("jongno_map.html")
    print("✅ 시각화 완료! 'jongno_map.html' 파일을 브라우저로 여세요.")

if __name__ == "__main__":
    visualize()