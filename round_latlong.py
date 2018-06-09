#PATH1 = "ghcnd_all/"
#PATH2 = "part/"
import multiprocessing as mp

IF_NAME = "ghcnd-stations.txt"
OF_NAME = "rnd-ghcnd-stations.txt"


if __name__ == "__main__":
    ifile = open(IF_NAME, "r")
    ofile = open(OF_NAME, "a")
    for line in ifile:
        l_tmp = line.split()
        l_tmp[1] = str(round(float(l_tmp[1]), 2))
        l_tmp[2] = str(round(float(l_tmp[2]), 2))
        temp = ";".join(l_tmp)
        ofile.write(temp + "\n")
    ofile.close()
    ifile.close()

