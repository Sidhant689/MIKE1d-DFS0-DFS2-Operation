using System;
using System.Data;
using DHI.Generic.MikeZero;
using DHI.Generic.MikeZero.DFS;
using DHI.Generic.MikeZero.DFS.dfs0;
using DHI.Generic.MikeZero.DFS.dfs123;
using Npgsql;
using static DHI.Mike1D.Generic.Collections.ValueIdList;
using static DHI.Mike1D.ResultDataAccess.AsciiBridge;

namespace Res1dDataUpload.Services
{
    public class DFS2Operations
    {
        private readonly IConfiguration _configuration;
        private readonly string _constr;

        public DFS2Operations(IConfiguration configuration)
        {
            _configuration = configuration;
            _constr = _configuration.GetConnectionString("ChennaiServerGalaxy");
        }

        public string GenerateDFS2Deterministic(string frDate, string fRun, string org)
        {
            try
            {
                using (NpgsqlConnection connection = new NpgsqlConnection(_constr))
                {
                    connection.Open();

                    var tableName = "";
                    string sqlSelectQuery = "";

                    // Declare an array to store itemNmaes

                    string[] itemNames = new string[] { };

                    if (org.Equals("ECMWF"))
                    {
                        tableName = "ECMWFFCastGridTwoDimensionalDeterministic";

                        sqlSelectQuery = $@"
                        SELECT
                            ""ECMWFC0000Ctrl"", ""ECMWFC0001Ctrl"", ""ECMWFC0002Ctrl"", ""ECMWFC0003Ctrl"", ""ECMWFC0004Ctrl"",
                            ""ECMWFC0100Ctrl"", ""ECMWFC0101Ctrl"", ""ECMWFC0102Ctrl"", ""ECMWFC0103Ctrl"", ""ECMWFC0104Ctrl"",
                            ""ECMWFC0200Ctrl"", ""ECMWFC0201Ctrl"", ""ECMWFC0202Ctrl"", ""ECMWFC0203Ctrl"", ""ECMWFC0204Ctrl"",
                            ""ECMWFC0300Ctrl"", ""ECMWFC0301Ctrl"", ""ECMWFC0302Ctrl"", ""ECMWFC0303Ctrl"", ""ECMWFC0304Ctrl"",
                            ""ECMWFC0400Ctrl"", ""ECMWFC0401Ctrl"", ""ECMWFC0402Ctrl"", ""ECMWFC0403Ctrl"", ""ECMWFC0404Ctrl"",
                            ""ECMWFC0500Ctrl"", ""ECMWFC0501Ctrl"", ""ECMWFC0502Ctrl"", ""ECMWFC0503Ctrl"", ""ECMWFC0504Ctrl"",
                            ""ECMWFC0600Ctrl"", ""ECMWFC0601Ctrl"", ""ECMWFC0602Ctrl"", ""ECMWFC0603Ctrl"", ""ECMWFC0604Ctrl"",
                            ""ECMWFC0700Ctrl"", ""ECMWFC0701Ctrl"", ""ECMWFC0702Ctrl"", ""ECMWFC0703Ctrl"", ""ECMWFC0704Ctrl"",
                            ""ECMWFC0800Ctrl"", ""ECMWFC0801Ctrl"", ""ECMWFC0802Ctrl"", ""ECMWFC0803Ctrl"", ""ECMWFC0804Ctrl""
                        FROM public.""{tableName}""
                        WHERE ""FRDate""='{frDate}' AND ""FRun""='{fRun}'";

                        itemNames = new string[]
                        {
                            "C0000Ctrl", "C0001Ctrl", "C0002Ctrl", "C0003Ctrl", "C0004Ctrl",
                            "C0100Ctrl", "C0101Ctrl", "C0102Ctrl", "C0103Ctrl", "C0104Ctrl",
                            "C0200Ctrl", "C0201Ctrl", "C0202Ctrl", "C0203Ctrl", "C0204Ctrl",
                            "C0300Ctrl", "C0301Ctrl", "C0302Ctrl", "C0303Ctrl", "C0304Ctrl",
                            "C0400Ctrl", "C0401Ctrl", "C0402Ctrl", "C0403Ctrl", "C0404Ctrl",
                            "C0500Ctrl", "C0501Ctrl", "C0502Ctrl", "C0503Ctrl", "C0504Ctrl",
                            "C0600Ctrl", "C0601Ctrl", "C0602Ctrl", "C0603Ctrl", "C0604Ctrl",
                            "C0700Ctrl", "C0701Ctrl", "C0702Ctrl", "C0703Ctrl", "C0704Ctrl",
                            "C0800Ctrl", "C0801Ctrl", "C0802Ctrl", "C0803Ctrl", "C0804Ctrl"
                        };
                    }
                    else if (org.Equals("NCEPGFS"))
                    {
                        tableName = "NCEPGFSFCastGridTwoDimensionalDeterministic";

                        sqlSelectQuery = $@"
                        SELECT
                            
                        FROM public.""{tableName}""
                        WHERE ""FRDate""='{frDate}' AND ""FRun""='{fRun}'";

                        itemNames = new string[]
                        {
                            "NCEPGFSC0000Ctrl", "NCEPGFSC0001Ctrl", "NCEPGFSC0002Ctrl", "NCEPGFSC0100Ctrl", "NCEPGFSC0101Ctrl",
                            "NCEPGFSC0102Ctrl", "NCEPGFSC0200Ctrl", "NCEPGFSC0201Ctrl", "NCEPGFSC0202Ctrl", "NCEPGFSC0300Ctrl",
                            "NCEPGFSC0301Ctrl", "NCEPGFSC0302Ctrl"
                        };
                    }



                    DataTable ecmwfWsData = new DataTable();
                    NpgsqlCommand cmd = new NpgsqlCommand(sqlSelectQuery, connection);
                    NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
                    adp.Fill(ecmwfWsData);


                    if (ecmwfWsData.Rows.Count == 0)
                    {
                        return $"Data is not present for FRDate: {frDate} and FRun: {fRun}";
                    }
                    else
                    {
                        // Create DFS2 file
                        float x0 = 79.89999974583402f;
                        float y0 = 12.500001526228607f;

                        string[] dfsCoordinate = { "LONG/LAT", x0.ToString(), y0.ToString(), "0" };
                        float dfsDx = 0.1f;
                        float dfsDy = 0.1f;
                        int dfsDt = 3600;

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

                        float[,,] ecmwfGridData = new float[72, 9, 5];
                        int ni = 0;

                        foreach (DataRow row in ecmwfWsData.Rows)
                        {
                            int columnIndex = 0;
                            for (int i = 0; i < 9; i++)
                            {
                                for (int j = 0; j < 5; j++)
                                {
                                    // Round the value obtained from the row and assign it to the grid data
                                    ecmwfGridData[ni, i, j] = (float)Math.Round(row.Field<float>(columnIndex), 1);
                                    columnIndex++;
                                }
                            }
                            ni++;
                        }


                        DfsFactory factory = new DfsFactory();
                        Dfs2Builder builder = Dfs2Builder.Create("ECMWFGridData", "dfs Timeseries Bridge", 0);

                        // Set up the header
                        builder.SetDataType(1);
                        builder.SetGeographicalProjection(factory.CreateProjectionGeoOrigin("UTM-33", 12.438741600559766, 55.225707842436385, 326.99999999999955));
                        builder.SetTemporalAxis(factory.CreateTemporalEqCalendarAxis(eumUnit.eumUminute, dfsstarttime, 0, 60));
                        builder.SetSpatialAxis(factory.CreateAxisEqD2(eumUnit.eumUmeter, ecmwfGridData.GetLength(2), 0, dfsDy, ecmwfGridData.GetLength(1), 0, dfsDx));
                        builder.DeleteValueFloat = -1e-30f;

                        // Add custom block
                        // M21_Misc : {orientation (should match projection), drying depth, -900=has projection, land value, 0, 0, 0}
                        builder.AddCustomBlock(factory.CreateCustomBlock("M21_Misc", new float[] { 327f, 0.2f, -900f, 10f, 0f, 0f, 0f }));

                        // Set up dynamic items
                        foreach (var item in itemNames)
                        {
                            builder.AddDynamicItem(org.ToString() + item, eumQuantity.Create(eumItem.eumIRainfall, eumUnit.eumUmillimeter), DfsSimpleType.Float, DataValueType.Instantaneous);
                        }

                        string dfsfilename = Path.Combine($"D:\\Chennai\\DFS\\DFSFiles_Exported\\TwoDimensional\\{org}\\DFS2\\Deterministic\\{frDate}\\{fRun}", $"AllWSDFS_{org}_{frDate}_{fRun}_Ctrl_File.dfs2");
                        // Create file
                        builder.CreateFile(dfsfilename);

                        // Get the file
                        Dfs2File file = builder.GetFile();

                        // Loop over all time steps
                        for (int i = 0; i < 72; i++)
                        {
                            // Loop over all items
                            for (int j = 0; j < itemNames.Length; j++)
                            {
                                // Create item data from the provided data array
                                IDfsItemData2D<float> itemData2D = (IDfsItemData2D<float>)file.CreateEmptyItemData(j + 1);

                                // Transpose the 2D array
                                float[,] transposedData = new float[ecmwfGridData.GetLength(1), ecmwfGridData.GetLength(2)];
                                for (int k = 0; k < ecmwfGridData.GetLength(2); k++)
                                {
                                    for (int l = 0; l < ecmwfGridData.GetLength(1); l++)
                                    {
                                        transposedData[l, k] = ecmwfGridData[i, l, k];
                                    }
                                }

                                // Reverse the order of the rows in the transposed data
                                float[,] reversedData = new float[transposedData.GetLength(0), transposedData.GetLength(1)];
                                int rowCount = transposedData.GetLength(0);
                                for (int k = 0; k < rowCount; k++)
                                {
                                    for (int l = 0; l < transposedData.GetLength(1); l++)
                                    {
                                        reversedData[rowCount - 1 - k, l] = transposedData[k, l];
                                    }
                                }


                                // Create a 1D array of the expected data type
                                int totalElements = reversedData.GetLength(0) * reversedData.GetLength(1);
                                float[] data = new float[totalElements];

                                // Copy data from transposedData to the 1D array
                                int index = 0;
                                for (int k = 0; k < reversedData.GetLength(0); k++)
                                {
                                    for (int l = 0; l < reversedData.GetLength(1); l++)
                                    {
                                        data[index++] = (float)Math.Round(reversedData[k, l], 1);
                                    }
                                }

                                //var data1 = data.Reverse();

                                file.WriteItemTimeStep(j + 1, i, 60, data);
                            }
                        }
                        file.Close();
                        return "DFS2 file generated successfully";
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }

        public string GenerateDFS2Ensemble(string? frDate, string? fRun, string org, string? ensemblePercentages)
        {
            try
            {

                var tablename = "";

                if (org.Equals("ECMWF"))
                {
                    tablename = "ECMWFFCastGridTwoDimensionalEnsemble";
                }
                else if (org.Equals("NCEPGFS"))
                {
                    tablename = "NCEPGFSFCastOneDimensionalEnsembleAllWS";
                }

                //string columns = @"""ECMWFC0000P{0}"", ""ECMWFC0001P{0}"", ""ECMWFC0002P{0}"", ""ECMWFC0003P{0}"", ""ECMWFC0004P{0}"",
                //            ""ECMWFC0100P{0}"", ""ECMWFC0101P{0}"", ""ECMWFC0102P{0}"", ""ECMWFC0103P{0}"", ""ECMWFC0104P{0}"",
                //            ""ECMWFC0200P{0}"", ""ECMWFC0201P{0}"", ""ECMWFC0202P{0}"", ""ECMWFC0203P{0}"", ""ECMWFC0204P{0}"",
                //            ""ECMWFC0300P{0}"", ""ECMWFC0301P{0}"", ""ECMWFC0302P{0}"", ""ECMWFC0303P{0}"", ""ECMWFC0304P{0}"",
                //            ""ECMWFC0400P{0}"", ""ECMWFC0401P{0}"", ""ECMWFC0402P{0}"", ""ECMWFC0403P{0}"", ""ECMWFC0404P{0}"",
                //            ""ECMWFC0500P{0}"", ""ECMWFC0501P{0}"", ""ECMWFC0502P{0}"", ""ECMWFC0503P{0}"", ""ECMWFC0504P{0}"",
                //            ""ECMWFC0600P{0}"", ""ECMWFC0601P{0}"", ""ECMWFC0602P{0}"", ""ECMWFC0603P{0}"", ""ECMWFC0604P{0}"",
                //            ""ECMWFC0700P{0}"", ""ECMWFC0701P{0}"", ""ECMWFC0702P{0}"", ""ECMWFC0703P{0}"", ""ECMWFC0704P{0}"",
                //            ""ECMWFC0800P{0}"", ""ECMWFC0801P{0}"", ""ECMWFC0802P{0}"", ""ECMWFC0803P{0}"", ""ECMWFC0804P{0}""";

                string columns = @"""ECMWFC0000P{0}"", ""ECMWFC0001P{0}"", ""ECMWFC0002P{0}"", ""ECMWFC0003P{0}"",
                            ""ECMWFC0100P{0}"", ""ECMWFC0101P{0}"", ""ECMWFC0102P{0}"", ""ECMWFC0103P{0}"", 
                            ""ECMWFC0200P{0}"", ""ECMWFC0201P{0}"", ""ECMWFC0202P{0}"", ""ECMWFC0203P{0}"", 
                            ""ECMWFC0300P{0}"", ""ECMWFC0301P{0}"", ""ECMWFC0302P{0}"", ""ECMWFC0303P{0}"",
                            ""ECMWFC0400P{0}"", ""ECMWFC0401P{0}"", ""ECMWFC0402P{0}"", ""ECMWFC0403P{0}""";


                // Define ensemble percentages based on the type parameter
                List<string> Perctntages = new List<string>();
                // If nothing is passed from the frontend
                if (string.IsNullOrEmpty(ensemblePercentages))
                {
                    Perctntages.AddRange(new string[] { "25", "50", "60", "75", "90", "95", "100" });
                }

                else
                {
                    Perctntages = ensemblePercentages.Split(',').ToList();
                }

                // Parse ensemblePercentages string into an array of integers

                foreach (var percentage in Perctntages)
                {
                    using (NpgsqlConnection connection = new NpgsqlConnection(_constr))
                    {
                        connection.Open();

                        string sqlSelectQuery = $"SELECT {string.Format(columns, percentage)} FROM public.\"{tablename}\" WHERE \"FRDate\" = '{frDate}' and \"FRun\" = '{fRun}' order by \"FDTime\"";

                        // Declare an array to store itemNmaes

                        string[] itemNames = new string[] { };

                        if (org.Equals("ECMWF"))
                        {
                            itemNames = new string[]
                            {
                            "ECMWFC0000P{0}", "ECMWFC0001P{0}", "ECMWFC0002P{0}", "ECMWFC0003P{0}", "ECMWFC0004P{0}",
                            "ECMWFC0100P{0}", "ECMWFC0101P{0}", "ECMWFC0102P{0}", "ECMWFC0103P{0}", "ECMWFC0104P{0}",
                            "ECMWFC0200P{0}", "ECMWFC0201P{0}", "ECMWFC0202P{0}", "ECMWFC0203P{0}", "ECMWFC0204P{0}",
                            "ECMWFC0300P{0}", "ECMWFC0301P{0}", "ECMWFC0302P{0}", "ECMWFC0303P{0}", "ECMWFC0304P{0}",
                            "ECMWFC0400P{0}", "ECMWFC0401P{0}", "ECMWFC0402P{0}", "ECMWFC0403P{0}", "ECMWFC0404P{0}",
                            "ECMWFC0500P{0}", "ECMWFC0501P{0}", "ECMWFC0502P{0}", "ECMWFC0503P{0}", "ECMWFC0504P{0}",
                            "ECMWFC0600P{0}", "ECMWFC0601P{0}", "ECMWFC0602P{0}", "ECMWFC0603P{0}", "ECMWFC0604P{0}",
                            "ECMWFC0700P{0}", "ECMWFC0701P{0}", "ECMWFC0702P{0}", "ECMWFC0703P{0}", "ECMWFC0704P{0}",
                            "ECMWFC0800P{0}", "ECMWFC0801P{0}", "ECMWFC0802P{0}", "ECMWFC0803P{0}", "ECMWFC0804P{0}"
                            };
                        }
                        else if (org.Equals("NCEPGFS"))
                        {
                            itemNames = new string[]
                            {
                            "NCEPGFSC0000Ctrl", "NCEPGFSC0001Ctrl", "NCEPGFSC0002Ctrl", "NCEPGFSC0100Ctrl", "NCEPGFSC0101Ctrl",
                            "NCEPGFSC0102Ctrl", "NCEPGFSC0200Ctrl", "NCEPGFSC0201Ctrl", "NCEPGFSC0202Ctrl", "NCEPGFSC0300Ctrl",
                            "NCEPGFSC0301Ctrl", "NCEPGFSC0302Ctrl"
                            };
                        }

                        DataTable ncepgfsWsData = new DataTable();
                        NpgsqlCommand cmd = new NpgsqlCommand(sqlSelectQuery, connection);
                        NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
                        adp.Fill(ncepgfsWsData);


                        if (ncepgfsWsData.Rows.Count == 0)
                        {
                            return $"Data is not present for FRDate: {frDate} and FRun: {fRun}";
                        }
                        else
                        {
                            // Create DFS2 file
                            float x0 = 79.89999974583402f;
                            float y0 = 12.500001526228607f;

                            string[] dfsCoordinate = { "LONG/LAT", x0.ToString(), y0.ToString(), "0" };
                            float dfsDx = 0.1f;
                            float dfsDy = 0.1f;
                            int dfsDt = 3600;

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

                            //float[,,] ecmwfGridData = new float[72, 9, 5];
                            //int ni = 0;

                            //foreach (DataRow row in ncepgfsWsData.Rows)
                            //{
                            //    int columnIndex = 0;
                            //    for (int i = 0; i < 9; i++)
                            //    {
                            //        for (int j = 0; j < 5; j++)
                            //        {
                            //            // Round the value obtained from the row and assign it to the grid data
                            //            ecmwfGridData[ni, i, j] = (float)Math.Round(row.Field<float>(columnIndex), 1);
                            //            columnIndex++;
                            //        }
                            //    }
                            //    ni++;
                            //}

                            float[,,] ecmwfGridData = new float[72, 5, 4];
                            int ni = 0;

                            foreach (DataRow row in ncepgfsWsData.Rows)
                            {
                                int columnIndex = 0;
                                for (int i = 0; i < 5; i++)
                                {
                                    for (int j = 0; j < 4; j++)
                                    {
                                        // Round the value obtained from the row and assign it to the grid data
                                        ecmwfGridData[ni, i, j] = (float)Math.Round(row.Field<float>(columnIndex), 1);
                                        columnIndex++;
                                    }
                                }
                                ni++;
                            }


                            DfsFactory factory = new DfsFactory();
                            Dfs2Builder builder = Dfs2Builder.Create("ECMWFGridData", "dfs Timeseries Bridge", 0);

                            // Set up the header
                            builder.SetDataType(1);
                            builder.SetGeographicalProjection(factory.CreateProjectionGeoOrigin("UTM-33", 12.438741600559766, 55.225707842436385, 326.99999999999955));
                            builder.SetTemporalAxis(factory.CreateTemporalEqCalendarAxis(eumUnit.eumUminute, dfsstarttime, 0, 60));
                            builder.SetSpatialAxis(factory.CreateAxisEqD2(eumUnit.eumUmeter, ecmwfGridData.GetLength(2), 0, dfsDy, ecmwfGridData.GetLength(1), 0, dfsDx));
                            builder.DeleteValueFloat = -1e-30f;

                            // Add custom block
                            // M21_Misc : {orientation (should match projection), drying depth, -900=has projection, land value, 0, 0, 0}
                            builder.AddCustomBlock(factory.CreateCustomBlock("M21_Misc", new float[] { 327f, 0.2f, -900f, 10f, 0f, 0f, 0f }));

                            // Set up dynamic items
                            foreach (var item in itemNames)
                            {
                                builder.AddDynamicItem(string.Format( item, percentage), eumQuantity.Create(eumItem.eumIRainfall, eumUnit.eumUmillimeter), DfsSimpleType.Float, DataValueType.Instantaneous);
                            }

                            string dfsfilename = Path.Combine($"D:\\Chennai\\DFS\\DFSFiles_Exported\\TwoDimensional\\{org}\\DFS2\\Ensemble\\{frDate}\\{fRun}", $"AllWSDFS_{org}_{frDate}_{fRun}_Ens_{percentage}_File.dfs2");
                            // Create file
                            builder.CreateFile(dfsfilename);

                            // Get the file
                            Dfs2File file = builder.GetFile();

                            // Loop over all time steps
                            for (int i = 0; i < 72; i++)
                            {
                                // Loop over all items
                                for (int j = 0; j < itemNames.Length; j++)
                                {
                                    // Create item data from the provided data array
                                    IDfsItemData2D<float> itemData2D = (IDfsItemData2D<float>)file.CreateEmptyItemData(j + 1);

                                    // Transpose the 2D array
                                    float[,] transposedData = new float[ecmwfGridData.GetLength(1), ecmwfGridData.GetLength(2)];
                                    for (int k = 0; k < ecmwfGridData.GetLength(2); k++)
                                    {
                                        for (int l = 0; l < ecmwfGridData.GetLength(1); l++)
                                        {
                                            transposedData[l, k] = ecmwfGridData[i, l, k];
                                        }
                                    }

                                    // Reverse the order of the rows in the transposed data
                                    float[,] reversedData = new float[transposedData.GetLength(0), transposedData.GetLength(1)];
                                    int rowCount = transposedData.GetLength(0);
                                    for (int k = 0; k < rowCount; k++)
                                    {
                                        for (int l = 0; l < transposedData.GetLength(1); l++)
                                        {
                                            reversedData[rowCount - 1 - k, l] = transposedData[k, l];
                                        }
                                    }


                                    // Create a 1D array of the expected data type
                                    int totalElements = reversedData.GetLength(0) * reversedData.GetLength(1);
                                    float[] data = new float[totalElements];

                                    // Copy data from transposedData to the 1D array
                                    int index = 0;
                                    for (int k = 0; k < reversedData.GetLength(0); k++)
                                    {
                                        for (int l = 0; l < reversedData.GetLength(1); l++)
                                        {
                                            data[index++] = (float)Math.Round(reversedData[k, l], 1);
                                        }
                                    }

                                    file.WriteItemTimeStep(j + 1, i, 60, data);
                                }
                            }
                            file.Close();
                        }
                    }
                }
                return $"{Perctntages.Count} DFS2 Ensemble files generated successfully";
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
    }
}