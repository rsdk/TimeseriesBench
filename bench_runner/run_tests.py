import subprocess
import time
import csv


class benchmark_runner:
    def __init__(self, name, lines_list, iSensors_list, bufsize_list):
        self.lines_list = lines_list
        self.iSensors_list = iSensors_list
        self.bufsize_list = bufsize_list
        self.name = name
        self.results = list()

    def run_once(self, lines, iSensors, bufsize):
        iValue = int(lines / iSensors)
        subprocess.call(["java", "-jar", self.name,  str(iValue), str(iSensors), str(bufsize)])
        with open("results.txt") as f:
            result = f.readline()
            self.results.append(result.strip().split(";"))
        time.sleep(20)

    def run_times(self, lines, iSensors, bufsize, count):
        for i in range(count):
            self.run_once(lines, iSensors, bufsize)

    def run_params(self, count=3):
        i = 0
        for lines in self.lines_list:
            for iSensor in self.iSensors_list:
                for bufsize in self.bufsize_list:
                    i += 1
                    print(i,'  lines:', lines, ' threads:', iSensor, ' buffersize:', bufsize)
                    self.run_times(lines, iSensor, bufsize, count)

    def save_results(self):
        with open('benchmark_' + self.name + '.csv', 'w', newline='') as f:
            csvw = csv.writer(f, delimiter=';')
            csvw.writerow(['name', 'used_time', 'lines', 'iValues', 'threads', 'buffersize'])
            csvw.writerows(self.results)


def main():
    mybm = benchmark_runner('mongo_java_async.jar', [10000000], [1, 2, 4], [1, 10, 100, 1000, 10000])
    mybm.run_params(5)
    mybm.save_results()


if __name__ == "__main__":
    main()
