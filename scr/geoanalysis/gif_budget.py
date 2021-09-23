import glob
from PIL import Image
from datetime import timedelta, date, datetime
# filepaths

breakpointList = [date(2018, 1, 1), date(2018, 5, 1), date(2018, 9, 1), date(
    2019, 1, 1), date(2019, 5, 1), date(2019, 9, 1), date(2020, 1, 1)]
timepointList = [8, 12, 18]
budgetList = [i for i in range(5, 121, 5)]


for i in breakpointList:
    for j in timepointList: 
        fp_in = r"D:\Luyu\reliability\serious_reliability\pdfs\REA_project_REA_" + i.strftime("%Y%m%d") + "_" + str(j) + "_*.png"
        fp_out = r"D:\Luyu\reliability\serious_reliability\gifs\timebudget\REA_" + i.strftime("%Y%m%d") + "_" + str(j) + ".gif"

        name_list = []
        for k in budgetList:
            name_list.append(r"D:\Luyu\reliability\serious_reliability\pdfs\REA_project_REA_" + i.strftime("%Y%m%d") + "_" + str(j) + "_" + str(k) + ".png")

        # a = sorted(glob.glob(fp_in))
        # print(name_list)
        img, *imgs = [Image.open(f) for f in name_list]
        img.save(fp=fp_out, format='GIF', append_images=imgs,
                save_all=True, duration=500, loop=0)
    #     break
    # break