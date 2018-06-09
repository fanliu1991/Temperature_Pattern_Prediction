stations = "stations.txt"
PATH1 = "../data/ghcnd/ghcnd_all/"
PATH2 = "./part_data/"
f_stations = open(stations, "r")
l_stations = f_stations.readlines()
f_track = open(PATH2+'part_data_list.txt', 'w')

from os import listdir
from shutil import copyfile
for dly in listdir(PATH1):
  if dly+'\n' in l_stations:
    f_track.write(dly+'\n')
    copyfile(PATH1+dly, PATH2+dly)
