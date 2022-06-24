import glob
from PIL import Image, ImageDraw, ImageFont
from datetime import timedelta, date, datetime
# filepaths

breakpointList = [date(2018, 9, 1)]
timepointList = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
budgetList = [30]
print("asdf")

for i in breakpointList:
    for k in budgetList:
        img_list = []
        fp_out = r"D:\Luyu\reliability\football\outputs\\" + i.strftime("%Y%m%d") + "_" + str(k) + ".gif"
        for j in timepointList: 
            print(i,j,k)
            fp_int = r"D:\Luyu\reliability\football\outputs\football1_" + i.strftime("%Y%m%d") + "_" + str(j) + "_" + str(k) + ".png"
            fp_in = r"D:\Luyu\reliability\football\outputs\football_" + i.strftime("%Y%m%d") + "_" + str(j) + "_" + str(k) + ".png"
            img = Image.open(fp_in) 
            draw = ImageDraw.Draw(img)
            font = ImageFont.truetype("D:\\Luyu\\reliability\\serious_reliability\\"+"OpenSans-Regular.ttf", 30)
            # draw.text((x, y),"Sample Text",(r,g,b))
            draw.text((0, 0),"Time: " + str(j) + ":00",(0,0,0), font=font)
            img.save(fp_int)
            img_list.append(fp_int)
        # a = sorted(glob.glob(fp_in))
        print(img_list)
        img, *imgs = [Image.open(f) for f in img_list]
        img.save(fp=fp_out, format='GIF', append_images=imgs, save_all=True, duration=1000, loop=0)
    #     break
    # break