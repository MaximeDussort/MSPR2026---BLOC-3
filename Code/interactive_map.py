# interactive_map.py

import folium
import pandas as pd
import geopandas as gpd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_PATH = PROJECT_ROOT / "Gold"
GOLD_PATH.mkdir(exist_ok=True)

df = pd.read_csv(PROJECT_ROOT / "Bronze" / "pauvrete_normalized.csv")

print("Colonnes détectées :", df.columns.tolist())

# ==============================
# Détection automatique colonne géographique
# ==============================

geo_col = None

possible_geo_cols = [
    "code_departement",
    "departement",
    "code_dep",
    "code_region",
    "region",
    "geo"
]

for col in possible_geo_cols:
    if col in df.columns:
        geo_col = col
        break

if geo_col is None:
    raise ValueError("Aucune colonne géographique détectée.")

print(f"Colonne géographique utilisée : {geo_col}")

# ==============================
# Agrégation
# ==============================

df_group = df.groupby(geo_col)["valeur"].mean().reset_index()

# ==============================
# Charger GeoJSON département
# ==============================

geo_url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson"
gdf = gpd.read_file(geo_url)

gdf = gdf.rename(columns={"code": geo_col})

gdf = gdf.merge(df_group, on=geo_col, how="left")

# ==============================
# Carte
# ==============================

m = folium.Map(location=[46.8, 2.5], zoom_start=6)

folium.Choropleth(
    geo_data=gdf,
    data=gdf,
    columns=[geo_col, "valeur"],
    key_on=f"feature.properties.{geo_col}",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Indicateur moyen"
).add_to(m)

m.save(GOLD_PATH / "carte_interactive.html")

print("Carte interactive générée")