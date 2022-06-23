# This file is for visualizing the data in GDBs to PNG. The value for visualization is diff 

import multiprocessing
import sys
import arcpy
import pymongo
import numpy
from datetime import timedelta, date, datetime
arcpy.env.overwriteOutput = True

breakpointList = [date(2018, 9, 1)]
timepointList = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
budgetList = [30]

# predefined layer with symbology ready. The symobology is unique value. To satisfy the condition, you will need to make a field in the csv file called "tag" and tag = 1 means it's the room of interests. I do this in excel: basically create a new field and set all values to 1 and join. So after joining only the rooms of interests will have the tag=1 flag.
symbologyLayer = r"D:\Luyu\reliability\football\symbology.lyr"


for i in breakpointList:
    for j in timepointList:
        for k in budgetList:

            fileLocation = "football_" + \
                (i.strftime("%Y%m%d")) + "_" + str(j)
            # Visualization mxd. But it will not be saved. Just use it as an intermedium mxd.
            mxd = arcpy.mapping.MapDocument(
                r"D:\Luyu\reliability\football\football.mxd")
            # Find the data frame. vis.mxd is blank and created manually in the arcmap, so it will only have one data frame named "Layers"
            df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
            geoGDB = r"D:\Luyu\reliability\football\football_PPA.gdb"
            # arcpy.TableToTable_conversion(crosswalk, geoGDB, 'crosswalk')
            arcpy.env.workspace = geoGDB  # set the gdb as environment to save the long path
            # interior_space = arcpy.management.MakeFeatureLayer("interior_space_joined", "interior_space_lyr")
            # interior_space = arcpy.management.MakeFeatureLayer("interior_space_joined", "interior_space_lyr")
            rawLayer = arcpy.mapping.Layer(fileLocation)
            boundingBoxLayer = arcpy.mapping.Layer(r"D:\Luyu\reliability\football\bounding_box.lyr")

            # rawLayer.showLabels = True # show label on
            # lyr = arcpy.mapping.ListLayers(mxd, "", df)[0]

            # Add symbol field

            df.extent = boundingBoxLayer.getExtent()

            try:
                arcpy.management.AddField(rawLayer, "unrlblt", "DOUBLE")
            except:
                print("Field exists")
            arcpy.CalculateField_management(rawLayer, "unrlblt", "!normdiff_" + str(k) + "!", "PYTHON_9.3")

            # Apply the symbology from the symbology layer to the input layer
            arcpy.ApplySymbologyFromLayer_management(rawLayer, symbologyLayer)
            # arcpy.RefreshActiveView()
            # arcpy.RefreshTOC()
            arcpy.mapping.AddLayer(df, rawLayer)
            
            # mxd.saveACopy(r"D:\Luyu\reliability\serious_reliability\mxds" + "\\" + 
            #             fileLocation + ".mxd")  # save a copy just in case
            # new_mxd = arcpy.mapping.MapDocument(r"D:\Luyu\reliability\serious_reliability\mxds" + "\\" + 
            #             fileLocation + "_" +  str(k)+ ".mxd")
            for TextElement in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
                TextElement.text = "Time: " + str(j) + ":00"
            print(i, j, k)

            arcpy.mapping.ExportToPNG(mxd, r"D:\Luyu\reliability\football\outputs" + "\\" + fileLocation + "_" +  str(k)+ ".png")
            # arcpy.mapping.ExportToPDF(
            #     new_mxd, r"D:\Luyu\reliability\serious_reliability\pdfs" + "\\" + fileLocation + ".pdf")
            del mxd, rawLayer
        # break
