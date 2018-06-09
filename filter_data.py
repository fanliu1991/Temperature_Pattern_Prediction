PATH1 = "ghcnd_all/"
PATH2 = "part/"
import multiprocessing as mp
from os import listdir
def worker(start, end):
  for dly in listdir(PATH1):
    if (dly[0] >= start) & (dly[0] < end):
      f_i = open(PATH1 + dly, "r")
      l=f_i.readline()
      if int(l[11:15]) >= 1950:
        f_i.close()
        continue
      else:
        fo_name = dly[0:11]+"-part.dly"
        temp_fo = open(PATH2+fo_name, "w")
        for line in f_i:
          if int(line[11:15]) < 1950:
            continue
          else:
            temp_fo.write(line)
        temp_fo.close()
        f_i.close()

if __name__ == "__main__":
  jobs = []
  p = mp.Process(target=worker, args=("A","U",))
  p.start()
  p.join()
  p = mp.Process(target=worker, args=("U", "Z",))
  p.start()
  p.join()
