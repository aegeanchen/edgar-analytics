import csv
import datetime

f_log = open("log.csv", "r")
f_period = open("inactivity_period.txt", "r")
f_output = open("sessionization.txt", "w")

log_reader = csv.reader(f_log)
log_dic = []
session_period = f_period.read()

start_time = None

for line in log_reader:
    if log_reader.line_num == 1:
        log_key = line
        ip_index = log_key.index("ip")
        date_index = log_key.index("date")
        time_index = log_key.index("time")
        cik_index = log_key.index("cik")
        accession_index = log_key.index("accession")
        extention_index = log_key.index("extention")
    else:
        if start_time == None:
            session_dic = {}
            session_dic["duration"] = None
            session_dic["ip"] = line[ip_index]
            session_dic["start_date"] = line[date_index]
            session_dic["start_time"] = line[time_index]
            session_dic["end_date"] = line[date_index]
            session_dic["end_time"] = line[time_index]
            session_dic["count"] = 1
            time_str = line[date_index] + " " + line[time_index]
            start_time = time_str
            log_dic.append(session_dic)
        else:
            time_str = line[date_index] + " " + line[time_index]
            ip_flag = False
            for log_dic_itr in range(len(log_dic)):
                item = log_dic[log_dic_itr]
                if item["ip"] == line[ip_index]:
                    ip_flag = True
                    str1 = item["end_date"] + " " + item["end_time"]
                    str2 = line[date_index] + " " + line[time_index]
                    date_time1 = datetime.datetime.strptime(str1, "%Y-%m-%d %H:%M:%S")
                    date_time2 = datetime.datetime.strptime(str2, "%Y-%m-%d %H:%M:%S")
                    if (date_time2 - date_time1).total_seconds() <= float(session_period):
                        log_dic[log_dic_itr]["end_date"] = line[date_index]
                        log_dic[log_dic_itr]["end_time"] = line[time_index]
                        log_dic[log_dic_itr]["count"] += 1
                        break
                    else:
                        session_dic = {}
                        session_dic["duration"] = None
                        session_dic["ip"] = line[ip_index]
                        session_dic["start_date"] = line[date_index]
                        session_dic["start_time"] = line[time_index]
                        session_dic["end_date"] = line[date_index]
                        session_dic["end_time"] = line[time_index]
                        session_dic["count"] = 1
                        log_dic.append(session_dic)
                        str2 = item["start_date"] + " " + item["start_time"]
                        date_time2 = datetime.datetime.strptime(str2, "%Y-%m-%d %H:%M:%S")
                        item["duration"] = str((date_time2 - date_time1).total_seconds() + 1)
                        str_output = item["ip"] + "," + item["start_date"] + " " + item["start_time"] + "," \
                                + item["end_date"] + " " + item["end_time"] + "," + item["duration"] + "," + str(item["count"])
                        f_output.write(str_output)
                        f_output.write("\n")
                        del log_dic[log_dic_itr]
                        break
            if not ip_flag:
                session_dic = {}
                session_dic["duration"] = None
                session_dic["ip"] = line[ip_index]
                session_dic["start_date"] = line[date_index]
                session_dic["start_time"] = line[time_index]
                session_dic["end_date"] = line[date_index]
                session_dic["end_time"] = line[time_index]
                session_dic["count"] = 1
                log_dic.append(session_dic)
            if start_time != time_str:
                remain_session = []
                log_dic_tmp = []
                start_time = time_str
                for log_dic_itr in range(len(log_dic)):
                    item = log_dic[log_dic_itr]
                    str_datetime = item["end_date"] + " " + item["end_time"]
                    end_date_time = datetime.datetime.strptime(str_datetime, "%Y-%m-%d %H:%M:%S")
                    start_date_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                    if (start_date_time - end_date_time).total_seconds() <= float(session_period):
                        remain_session.append(log_dic_itr)
                    else:
                        start_str = item["start_date"] + " " + item["start_time"]
                        end_str = item["end_date"] + " " + item["end_time"]
                        start_date_time = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
                        end_date_time = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                        item["duration"] = str((end_date_time - start_date_time).total_seconds() + 1)
                        str_output = item["ip"] + "," + item["start_date"] + " " + item["start_time"] + "," \
                                     + item["end_date"] + " " + item["end_time"] + "," + item["duration"] + "," + str(item[
                                         "count"]) + "\n"
                        f_output.write(str_output)
                for i in range(len(remain_session)):
                    log_dic_tmp.append(log_dic[remain_session[i]])
                log_dic = log_dic_tmp


for log_dic_itr in range(len(log_dic)):
    item = log_dic[log_dic_itr]
    start_str = item["start_date"] + " " + item["start_time"]
    end_str = item["end_date"] + " " + item["end_time"]
    start_date_time = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
    end_date_time = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
    item["duration"] = str((end_date_time - start_date_time).total_seconds() + 1)
    str_output = item["ip"] + "," + item["start_date"] + " " + item["start_time"] + "," \
                 + item["end_date"] + " " + item["end_time"] + "," + item["duration"] + "," + str(item[
                     "count"]) + "\n"
    f_output.write(str_output)

f_log.close()
f_period.close()
f_period.close()


















