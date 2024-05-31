using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using DHI.Generic.MikeZero;
using DHI.Generic.MikeZero.DFS;
using DHI.Generic.MikeZero.DFS.dfs0;
using DHI.Generic.MikeZero.DFS.dfs123;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace Res1dDataUpload.Services
{
    public class DFS0Operations
    {
        private readonly IConfiguration _configuration;
        private readonly string _constr;

        public DFS0Operations(IConfiguration configuration)
        {
            _configuration = configuration;
            _constr = _configuration.GetConnectionString("ChennaiServerGalaxy");
        }

        public string GenerateDeterministicDFS0(string frDate, string fRun, string org)
        {
            try
            {
                using (NpgsqlConnection connection = new NpgsqlConnection(_constr))
                {
                    connection.Open();

                    
                    // Storing the data in datatable . You can store in array, list as per requirement
                    DataTable ArrrayData = new DataTable();
                    NpgsqlCommand cmd = new NpgsqlCommand(sqlSelectQuery, connection);
                    NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
                    adp.Fill(ArrrayData);

                    if (ArrrayData.Rows.Count == 0)
                    {
                        return $"Data is not present for FRDate: {frDate} and FRun: {fRun}";
                    }
                    else
                    {

                        // Define the item names for ECMWF weather stations
                        string[] itemNames = { 
                            // store the names as per the requirement 
                    };

                        var fDTime = "";
                        var frdate = $"{frDate.Substring(0, 4)}-{frDate.Substring(4, 2)}-{frDate.Substring(6, 2)}";
                        DateTime date = Convert.ToDateTime(frdate);

                        if (fRun.Equals("R00"))
                        {
                            fDTime = date.ToString("dd-MM-yyyy 06:30:00");
                        }
                        else if (fRun.Equals("R06"))
                        {
                            fDTime = date.ToString("dd-MM-yyyy 12:30:00");
                        }
                        else if (fRun.Equals("R12"))
                        {
                            fDTime = date.ToString("dd-MM-yyyy 18:30:00");
                        }
                        else if (fRun.Equals("R18"))
                        {
                            date = date.AddDays(1);
                            fDTime = date.ToString("dd-MM-yyyy 00:30:00");
                        }

                        // Calculate the start time based on FDTime
                        DateTime dfsstarttime = DateTime.ParseExact(fDTime, "dd-MM-yyyy HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

                        // Prepare the data arrays
                        double[] times = new double[ArrrayData.Rows.Count];
                        DateTime currentTime = dfsstarttime;

                        double[,] values = new double[ArrrayData.Rows.Count, 42]; // i am storing 72 rows and 42 columns
                        for (int i = 0; i < ArrrayData.Rows.Count; i++)
                        {
                            for (int j = 0; j < 42; j++)
                            {
                                values[i, j] = Math.Round(Convert.ToDouble(ArrrayData.Rows[i][j]), 1);
                            }
                        }

                        DfsFactory factory = new DfsFactory();
                        DfsBuilder builder = DfsBuilder.Create("ECMWFCastData", "dfs Timeseries Bridge", 10000);

                        // Set up file header
                        builder.SetDataType(1);
                        builder.SetGeographicalProjection(factory.CreateProjectionUndefined());
                        IDfsTemporalAxis temporalAxis = factory.CreateTemporalEqCalendarAxis(eumUnit.eumUminute, dfsstarttime, 0, 60); // I want every one hour time interval 
                        builder.SetTemporalAxis(temporalAxis);
                        builder.SetItemStatisticsType(StatType.RegularStat);

                        // Add dynamic items
                        for (int i = 0; i < itemNames.Length; i++) // 42
                        {
                            DfsDynamicItemBuilder item = builder.CreateDynamicItemBuilder();
                            item.Set(itemNames[i], eumQuantity.Create(eumItem.eumIRainfall, eumUnit.eumUmillimeter), DfsSimpleType.Double);
                            item.SetValueType(DataValueType.Instantaneous);
                            item.SetAxis(factory.CreateAxisEqD0());
                            builder.AddDynamicItem(item.GetDynamicItemInfo());
                        }

                        string dfsfilename = Path.Combine(// specifuy the path you want to store the file, filename.dfs0");

                        builder.CreateFile(dfsfilename);

                        IDfsFile file = builder.GetFile();

                        // Write data to file
                        Dfs0Util.WriteDfs0DataDouble(file, times, values);
                        file.Close();

                        return $"DFS file generated successfully";

                    }
                }
            }
            catch (Exception ex)
            {

                throw new Exception(ex.Message);
            }
        }
    }
}
