# -*- coding: utf-8 -*-
"""
Generated by ArcGIS ModelBuilder on : 2021-09-30 14:54:56

This file generates the flattened STP or overlayed PPAs with different time budgets. The output is a polygon layers with merged PPAs (which means one PPA is a separate and multipart layer) 
"""
import arcpy


budgetList = [i for i in range(10, 121, 10)]

def Model1():  # HIGBROS_vis

    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    arcpy.ImportToolbox(r"c:\program files\arcgis\pro\Resources\ArcToolbox\toolboxes\Data Management Tools.tbx")
    HIGBROS_csv = r"D:\Luyu\reliability\serious_reliability\raw_data\HIGBROS.csv"

    # Process: XY Table To Point (XY Table To Point) (management)
    Output_Feature_Class = "D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_XYTableToPoint1"
    arcpy.management.XYTableToPoint(in_table=HIGBROS_csv, out_feature_class=Output_Feature_Class, x_field="stop_lon", y_field="stop_lat", z_field="", coordinate_system="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision")

    # Process: Project (Project) (management)
    HIGBROS_XYTableToPoint1_Proj = "D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_Proj"
    arcpy.management.Project(in_dataset=Output_Feature_Class, out_dataset=HIGBROS_XYTableToPoint1_Proj, out_coor_system="PROJCS['NAD_1983_StatePlane_Ohio_South_FIPS_3402',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Lambert_Conformal_Conic'],PARAMETER['False_Easting',600000.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-82.5],PARAMETER['Standard_Parallel_1',38.73333333333333],PARAMETER['Standard_Parallel_2',40.03333333333333],PARAMETER['Latitude_Of_Origin',38.0],UNIT['Meter',1.0]]", transform_method=["WGS_1984_(ITRF00)_To_NAD_1983"], in_coor_system="GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", preserve_shape="NO_PRESERVE_SHAPE", max_deviation="", vertical="NO_VERTICAL")

    typeList = ["SC", "RT", "RV"]
    for eachType in typeList:
        bufferList = []
        for budget in budgetList:
            # Process: Select Layer By Attribute (Select Layer By Attribute) (management)
            HIGBROS_XYTableToPoint1_Proj1, Count = arcpy.management.SelectLayerByAttribute(in_layer_or_view=HIGBROS_XYTableToPoint1_Proj, selection_type="NEW_SELECTION", where_clause="time" + eachType + " <= " + str(budget * 60), invert_where_clause="")

            # Process: Copy Features (Copy Features) (management)
            HIGBROS_15 = "D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_" + str(budget)
            arcpy.management.CopyFeatures(in_features=HIGBROS_XYTableToPoint1_Proj1, out_feature_class=HIGBROS_15, config_keyword="", spatial_grid_1=None, spatial_grid_2=None, spatial_grid_3=None)

            # Process: Add Field (Add Field) (management)
            HIGBROS_15_2_ = arcpy.management.AddField(in_table=HIGBROS_15, field_name="budget", field_type="DOUBLE", field_precision=None, field_scale=None, field_length=None, field_alias="", field_is_nullable="NULLABLE", field_is_required="NON_REQUIRED", field_domain="")[0]

            # Process: Calculate Field (Calculate Field) (management)
            HIGBROS_15_4_ = arcpy.management.CalculateField(in_table=HIGBROS_15_2_, field="budget", expression="min(" + str(budget) + "*60*1.4 - !time" + eachType + "! * 1.4, 700)", expression_type="PYTHON3", code_block="", field_type="TEXT")[0]

            # Process: Buffer (Buffer) (analysis)
            HIGBROS_15_Buffer = "D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_" + str(budget) + "_Buffer_" + eachType
            arcpy.analysis.Buffer(in_features=HIGBROS_15_4_, out_feature_class=HIGBROS_15_Buffer, buffer_distance_or_field="budget", line_side="FULL", line_end_type="ROUND", dissolve_option="ALL", dissolve_field=[], method="GEODESIC")
            bufferList.append(HIGBROS_15_Buffer)
            print(eachType, budget)
        # Process: Merge (Merge) (management)
        HIGBROS_Merge = "D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_Merge_" + eachType
        arcpy.management.Merge(inputs=list(reversed(bufferList)), output=HIGBROS_Merge, field_mappings="startStopID \"startStopID\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,startStopID,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,startStopID,0,8000;receivingStopID \"receivingStopID\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,receivingStopID,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,receivingStopID,0,8000;timeRT \"timeRT\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,timeRT,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,timeRT,-1,-1;walkTimeRT \"walkTimeRT\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,walkTimeRT,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,walkTimeRT,-1,-1;busTimeRT \"busTimeRT\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,busTimeRT,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,busTimeRT,-1,-1;waitTimeRT \"waitTimeRT\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,waitTimeRT,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,waitTimeRT,-1,-1;generatingStopIDRT \"generatingStopIDRT\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,generatingStopIDRT,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,generatingStopIDRT,0,8000;lastTripIDRT \"lastTripIDRT\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,lastTripIDRT,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,lastTripIDRT,-1,-1;lastTripTypeRT \"lastTripTypeRT\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,lastTripTypeRT,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,lastTripTypeRT,0,8000;transferCountRT \"transferCountRT\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,transferCountRT,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,transferCountRT,-1,-1;visitTagRT \"visitTagRT\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,visitTagRT,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,visitTagRT,0,8000;timeSC \"timeSC\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,timeSC,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,timeSC,-1,-1;walkTimeSC \"walkTimeSC\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,walkTimeSC,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,walkTimeSC,-1,-1;busTimeSC \"busTimeSC\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,busTimeSC,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,busTimeSC,-1,-1;waitTimeSC \"waitTimeSC\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,waitTimeSC,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,waitTimeSC,-1,-1;generatingStopIDSC \"generatingStopIDSC\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,generatingStopIDSC,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,generatingStopIDSC,0,8000;lastTripIDSC \"lastTripIDSC\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,lastTripIDSC,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,lastTripIDSC,-1,-1;lastTripTypeSC \"lastTripTypeSC\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,lastTripTypeSC,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,lastTripTypeSC,0,8000;transferCountSC \"transferCountSC\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,transferCountSC,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,transferCountSC,-1,-1;visitTagSC \"visitTagSC\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,visitTagSC,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,visitTagSC,0,8000;stop_lat \"stop_lat\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,stop_lat,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,stop_lat,-1,-1;stop_lon \"stop_lon\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,stop_lon,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,stop_lon,-1,-1;revisitTag \"revisitTag\" true true false 8000 Text 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,revisitTag,0,8000,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,revisitTag,0,8000;timeRV \"timeRV\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,timeRV,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,timeRV,-1,-1;walkTimeRV \"walkTimeRV\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,walkTimeRV,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,walkTimeRV,-1,-1;waitTimeRV \"waitTimeRV\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,waitTimeRV,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,waitTimeRV,-1,-1;busTimeRV \"busTimeRV\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,busTimeRV,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,busTimeRV,-1,-1;lastTripIDRV \"lastTripIDRV\" true true false 4 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,lastTripIDRV,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,lastTripIDRV,-1,-1;budget \"budget\" true true false 8 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,budget,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,budget,-1,-1;BUFF_DIST \"BUFF_DIST\" true true false 0 Double 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,BUFF_DIST,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,BUFF_DIST,-1,-1;ORIG_FID \"ORIG_FID\" true true false 0 Long 0 0,First,#,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_15_Buffer,ORIG_FID,-1,-1,D:\\Luyu\\reliability\\serious_reliability\\PPA.gdb\\HIGBROS_30_Buffer,ORIG_FID,-1,-1", add_source="NO_SOURCE_INFO")

if __name__ == '__main__':
    # Global Environment settings
    with arcpy.EnvManager(scratchWorkspace=r"D:\Luyu\reliability\serious_reliability\PPA.gdb", workspace=r"D:\Luyu\reliability\serious_reliability\PPA.gdb"):
        Model1()
