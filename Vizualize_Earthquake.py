#Import Packages 
import folium
from folium.plugins import MarkerCluster
import pandas as pd
import configparser


#Loading the Config file
config = configparser.ConfigParser()
config.read('configfile.cfg')

path = config.get('OUTPUT','MERGED_FILE')

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

# Add markers to the cluster instead of directly to the map and pop message
for _, row in df.iterrows():
    color = color_scheme.get(row['Magnitude Category'], 'gray')
    popup_message = folium.Popup(f"Magnitude: {row['Magnitude']}, Depth: {row['Depth']}km, Category: {row['Magnitude Category']}", max_width=300)
    
    folium.CircleMarker(
        location=[row['Latitude'], row['Longitude']],
        radius=7,
        popup=popup_message,
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
