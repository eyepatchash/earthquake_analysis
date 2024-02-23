#Import Packages 
import folium
from folium.plugins import MarkerCluster
import pandas as pd


#File path for Processed Output csv
path = r'C:\Users\aswan\OneDrive\Documents\Earthquake_Analysis\data\output.csv'

# Load the processed earthquake data
df = pd.read_csv(path)


# Initialize the map with a custom tileset
map = folium.Map(location=[20, 0], tiles='CartoDB positron', zoom_start=2)

# Marker Cluster To Avoid CLuttering 
marker_cluster = MarkerCluster().add_to(map)

# USing a  color scheme for the magnitude categories
color_scheme = {
    'Low': 'green',
    'Moderate': 'orange',
    'High': 'red'
}

# Add markers to the cluster instead of directly to the map
for _, row in df.iterrows():
    color = color_scheme.get(row['Magnitude Category'], 'gray')
    folium.CircleMarker(
        location=[row['Latitude'], row['Longitude']],
        radius=7,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.7
    ).add_to(marker_cluster)

# Adding Legend using HTML
legend_html = '''
     <div style="position: fixed; 
     bottom: 50px; left: 50px; width: 150px; height: 90px; 
     border:2px solid grey; z-index:9999; font-size:14px;
     ">&nbsp; Magnitude Category <br>
     &nbsp; High &nbsp; <i class="fa fa-circle fa-1x" style="color:red"></i><br>
     &nbsp; Moderate &nbsp; <i class="fa fa-circle fa-1x" style="color:orange"></i><br>
     &nbsp; Low &nbsp; <i class="fa fa-circle fa-1x" style="color:green"></i>
      </div>
     '''
map.get_root().html.add_child(folium.Element(legend_html))

# Save the map
map.save('Earthquake_Distribution_Visualization_Map.html')
