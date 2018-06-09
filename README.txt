1. Download all ghcnd files to get dataset. They are the datasets for assistance. 
   The original data for analysis are not uploaded since they are about 26 GB. 
   Please connect one of our group member for original data if needed.
2. Use round_latlong.py to decrease the accuracy of stations' latitude and longitude. This is used later to find the Koppen climate classification of each station.
2. Use stations_means.py to cluster 100K+ stations into 2500 groups by latitude, longitude, elevation
3. Use stations_filter.py to abstract selected stations data from the whole.
3. Use filter_data.py to filter 1950 afterwards data.
4. Use stations_means.py to cluster 100K+ stations into 2500 groups by latitude, longitude, elevation
5. Use merge_month.py to split minimum and maximum data, merge monthly data into a whole year.
6. Use climate_patterns.py to find the number of different climate patterns
