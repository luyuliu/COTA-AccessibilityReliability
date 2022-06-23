import arcpy
from sys import argv
from datetime import timedelta, date, datetime
# arcpy.env.workspace = r"D:\Luyu\reliability\football\football_PPA.gdb"

def Visualize(file_name):  # Visualize
    rawLocation = "D:\\Luyu\\reliability\\football\\raw_data\\csvs\\"

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = False

    # Model Environment settings
    with arcpy.EnvManager(scratchWorkspace=r"D:\Luyu\reliability\football\football_PPA.gdb", workspace=r"D:\Luyu\reliability\football\football_PPA.gdb", overwriteOutput = True):
        # arcpy.env.overwriteOutput = True

        # Process: XY Table To Point (XY Table To Point) (management)
        REA_raw = "D:\\Luyu\\reliability\\football\\football_PPA.gdb\\raw_" + file_name
        arcpy.management.XYTableToPoint(in_table=rawLocation + file_name+".csv", out_feature_class=REA_raw, x_field="lon", y_field="lat", z_field="", coordinate_system="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision")

        # Process: Project (Project) (management)
        REA_project = "D:\\Luyu\\reliability\\football\\football_PPA.gdb\\" + file_name
        arcpy.management.Project(in_dataset=REA_raw, out_dataset=REA_project, out_coor_system="PROJCS['NAD_1983_StatePlane_Ohio_South_FIPS_3402',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',600000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-82.5],PARAMETER['Standard_Parallel_1',38.73333333333333],PARAMETER['Standard_Parallel_2',40.03333333333333],PARAMETER['Latitude_Of_Origin',38.0],UNIT['Meter',1.0]]", transform_method=["WGS_1984_(ITRF00)_To_NAD_1983"], in_coor_system="PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]", preserve_shape="NO_PRESERVE_SHAPE", max_deviation="", vertical="NO_VERTICAL")

        # Process: Add Fields (multiple) (Add Fields (multiple)) (management)
        
        budgetList = [i for i in range(0, 121, 5)]

        field_description = []

        for budget in budgetList:
            a = ["diff_" + str(budget), "DOUBLE", "", "", "", ""]
            b = ["normdiff_" + str(budget), "DOUBLE", "", "", "", ""]
            field_description.append(a)
            field_description.append(b)

        REA_raw_1 = arcpy.management.AddFields(in_table=REA_project, field_description=field_description)[0]

        # Process: Calculate Field (Calculate Field) (management)
        REA_raw_2 = arcpy.management.CalculateField(in_table=REA_raw_1, field="diff_30", expression="!PPA_SC_30! - !PPA_RV_30!", expression_type="PYTHON3", code_block="", field_type="TEXT")[0]

        # Process: Calculate Field (2) (Calculate Field) (management)
        for budget in budgetList:
            expression = "(!PPA_SC_" + str(budget) + "!-!PPA_RV_" + str(budget) + "!)"
            arcpy.management.CalculateField(in_table=REA_raw_2, field="diff_" + str(budget), expression=expression, expression_type="PYTHON3", code_block="", field_type="TEXT")
            norm_expression = "(!PPA_SC_" + str(budget) + "!-!PPA_RV_" + str(budget) + "!)/!PPA_SC_" + str(budget) + "!"
            arcpy.management.CalculateField(in_table=REA_raw_2, field="normdiff_" + str(budget), expression=norm_expression, expression_type="PYTHON3", code_block="", field_type="TEXT")

if __name__ == '__main__':
    breakpointList = [date(2018,9,8), date(2018,9,22), date(2018,10,6), date(2018,10,13), date(2018,11,3), date(2018,11,24), date(2019,8,31), date(2019,9,7), date(2019,9,21), date(2019,10,5), date(2019,10,26), date(2019,11,9), date(2019,11,23)]
    for i in (breakpointList):
        for j in [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]:
            fileLocation = "football_" + (i.strftime("%Y%m%d")) + "_" + str(j)
            Visualize(fileLocation)
            print(fileLocation)
