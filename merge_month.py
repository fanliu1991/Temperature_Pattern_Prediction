PATHIN = "../data/ghcnd/ghcnd_all/"
PATHOUT_MAX = "./temp-data/TMAX/"
PATHOUT_MIN = "./temp-data/TMIN/"
from os import listdir
from multiprocessing import Process

def cut_month_tail(month_temp):
  if month_temp[15:17] == "02":
    l = month_temp[21:245]
  if (month_temp[15:17] == "04") | (month_temp[15:17] == "06") |(month_temp[15:17] == "09") |(month_temp[15:17] == "11"):
    l = month_temp[21:261]
  l = month_temp[21:269]
  l = l.replace("  a", "   ").replace("  G", "   ").replace("  C", "   ").replace(" IG", "   ")
  return l

def worker(start, end):
  for file_name in listdir(PATHIN):
    if (file_name[0: ( 0 + len(start) )] < start) | (file_name[0: ( 0 + len(end) )] >= end):
      continue
    in_f=open(PATHIN + file_name, "r")
    out_fmax=open(PATHOUT_MAX + file_name, "w")
    out_fmin=open(PATHOUT_MIN + file_name, "w")

    first_line = in_f.readline()
    stationyear_max = first_line[11:15]
    stationyear_min = first_line[11:15]
    new_line_max = stationyear_max + "TMAX||"
    new_line_min = stationyear_min + "TMIN||"
    in_f.seek(0)

    for l in in_f:
      if int(l[11:15]) < 1950:
        stationyear_max = l[11:15]
        new_line_max = stationyear_max + "TMAX||"
        stationyear_min = l[11:15]
        new_line_min = stationyear_min + "TMIN||"
        continue
      if ((l[17:21] != "TMAX") & (l[17:21] != "TMIN")):
        continue

      current_stationyear = l[11:15]
      if (stationyear_max != current_stationyear) & (l[17:21]=="TMAX"):
        if len(new_line_max) > 2900:
          out_fmax.write(new_line_max + "\n")
        new_line_max = stationyear_max + "TMAX||" + cut_month_tail(l)#[21:269]
        stationyear_max = current_stationyear
      elif (stationyear_min != current_stationyear) & (l[17:21]=="TMIN"):
        if len(new_line_min) > 2900:
          out_fmin.write(new_line_min + "\n")
        new_line_min = stationyear_min + "TMIN||" + cut_month_tail(l)
        stationyear_min = current_stationyear
      else:
        if l[17:21] == "TMAX":
          new_line_max = new_line_max + cut_month_tail(l)
        elif l[17:21] == "TMIN":
          new_line_min = new_line_min + cut_month_tail(l)

    if len(new_line_max) > 2900:
      out_fmax.write(new_line_max + "\n")
    if len(new_line_min) > 2900:
      out_fmin.write(new_line_min + "\n")

    out_fmax.close()
    out_fmin.close()
    in_f.close()

if __name__ == "__main__":
  p1 = Process(target=worker, args=("A","CA",))
  p1.start()
  p2 = Process(target=worker, args=("CA", "U",))
  p2.start()
  p3 = Process(target=worker, args=("U", "V",))
  p3.start()
  p4 = Process(target=worker, args=("V", "Z",))
  p4.start()

  p1.join()
  p2.join()
  p3.join()
  p4.join()
