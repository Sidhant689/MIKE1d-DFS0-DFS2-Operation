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

                    var tablename = "";

                    if (org.Equals("ECMWF"))
                    {
                        tablename = "ECMWFFCastOneDimensionalAllWSDeterministic";
                    }
                    else if (org.Equals("NCEPGFS"))
                    {
                        tablename = "NCEPGFSFCastOneDimensionalAllWSDeterministic";
                    }

                    // SQL query to fetch ECMWF weather station data for the specific FRDate and FRun
                    string sqlSelectQuery = $"SELECT \"NemamWSCtrl\", \"SriperumbudurWSCtrl\", \"PillaipakkamWSCtrl\", \"GunduperumbeduWSCtrl\", " +
                            "\"OrathurWSCtrl\", \"GuduvancheryWSCtrl\", \"NandhivaramWSCtrl\", \"AdanurWSCtrl\", \"ManimangalamWSCtrl\", " +
                            "\"MalaipattuWSCtrl\", \"ChowdrikalWSCtrl\", \"ChembarambakkamWSCtrl\", \"CooumBigTankWSCtrl\", \"TiruvallurCWSCtrl\", \"PutlurWSCtrl\", " +
                            "\"AranvoyalWSCtrl\", \"KannapanpalayamWSCtrl\", \"AmmapalliWSCtrl\", \"NagariWSCtrl\", \"AyyaneriWSCtrl\", \"KodakkalWSCtrl\", " +
                            "\"MahendravadiWSCtrl\", \"KaveripakkamWSCtrl\", \"KesavaramWSCtrl\", \"KallarWSCtrl\", \"NandiWSCtrl\", \"RamapuramWSCtrl\", " +
                            "\"PalayanurWSCtrl\", \"PoondiWSCtrl\", \"TamaraipakkamWSCtrl\", \"TiruvallurKWSCtrl\", \"KattankalWSCtrl\", \"KaranodaiWSCtrl\", " +
                            "\"AvadiWSCtrl\", \"CholavaramWSCtrl\", \"RedhillsWSCtrl\", " +
                            "\"MahabalipuramWSCtrl\", \"ManamathyWSCtrl\", \"KondangiWSCtrl\", \"ThiruporurWSCtrl\", \"ThaiyurWSCtrl\", \"AgaramthenWSCtrl\" " +
                            $"FROM public.\"{tablename}\" Where \"FRDate\"='{frDate}' and \"FRun\"='{fRun}'";

                    DataTable ECMWFWSData = new DataTable();
                    NpgsqlCommand cmd = new NpgsqlCommand(sqlSelectQuery, connection);
                    NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
                    adp.Fill(ECMWFWSData);

                    if (ECMWFWSData.Rows.Count == 0)
                    {
                        return $"Data is not present for FRDate: {frDate} and FRun: {fRun}";
                    }
                    else
                    {

                        // Define the item names for ECMWF weather stations
                        string[] itemNames = { "NemamWSCtrl", "SriperumbudurWSCtrl", "PillaipakkamWSCtrl", "GunduperumbeduWSCtrl",
                            "OrathurWSCtrl", "GuduvancheryWSCtrl", "NandhivaramWSCtrl", "AdanurWSCtrl", "ManimangalamWSCtrl",
                            "MalaipattuWSCtrl", "ChowdrikalWSCtrl", "ChembarambakkamWSCtrl", "CooumBigTankWSCtrl", "TiruvallurCWSCtrl",
                            "PutlurWSCtrl", "AranvoyalWSCtrl", "KannapanpalayamWSCtrl", "AmmapalliWSCtrl", "NagariWSCtrl", "AyyaneriWSCtrl",
                            "KodakkalWSCtrl", "MahendravadiWSCtrl", "KaveripakkamWSCtrl", "KesavaramWSCtrl", "KallarWSCtrl", "NandiWSCtrl",
                            "RamapuramWSCtrl", "PalayanurWSCtrl", "PoondiWSCtrl", "TamaraipakkamWSCtrl", "TiruvallurKWSCtrl", "KattankalWSCtrl",
                            "KaranodaiWSCtrl", "AvadiWSCtrl", "CholavaramWSCtrl", "RedhillsWSCtrl", "MahabalipuramWSCtrl", "ManamathyWSCtrl",
                            "KondangiWSCtrl", "ThiruporurWSCtrl", "ThaiyurWSCtrl", "AgaramthenWSCtrl"
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
                        double[] times = new double[ECMWFWSData.Rows.Count];
                        DateTime currentTime = dfsstarttime;

                        double[,] values = new double[ECMWFWSData.Rows.Count, 42];
                        for (int i = 0; i < ECMWFWSData.Rows.Count; i++)
                        {
                            for (int j = 0; j < 42; j++)
                            {
                                values[i, j] = Math.Round(Convert.ToDouble(ECMWFWSData.Rows[i][j]), 1);
                            }
                        }

                        DfsFactory factory = new DfsFactory();
                        DfsBuilder builder = DfsBuilder.Create("ECMWFCastData", "dfs Timeseries Bridge", 10000);

                        int nt = 72; // Number of time steps

                        // Set up file header
                        builder.SetDataType(1);
                        builder.SetGeographicalProjection(factory.CreateProjectionUndefined());
                        IDfsTemporalAxis temporalAxis = factory.CreateTemporalEqCalendarAxis(eumUnit.eumUminute, dfsstarttime, 0, 60);//nt,ECMWFWSData.Rows.Count
                        builder.SetTemporalAxis(temporalAxis);
                        builder.SetItemStatisticsType(StatType.RegularStat);

                        // Add dynamic items
                        for (int i = 0; i < itemNames.Length; i++)
                        {
                            DfsDynamicItemBuilder item = builder.CreateDynamicItemBuilder();
                            item.Set(itemNames[i], eumQuantity.Create(eumItem.eumIRainfall, eumUnit.eumUmillimeter), DfsSimpleType.Double);
                            item.SetValueType(DataValueType.Instantaneous);
                            item.SetAxis(factory.CreateAxisEqD0());
                            builder.AddDynamicItem(item.GetDynamicItemInfo());
                        }

                        //var docPath = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
                        //docPath = Path.Combine(docPath, "RTFFModelPyWorkings\\OneDimensional\\ECMWF\\Control\\OneDimOutputAllWS\\DFSInput\\DFS0");

                        //string dfsfilename = Path.Combine(docPath, $"AllWSDFS_{frDate}_{fRun}_CtrlFile.dfs0");

                        string dfsfilename = Path.Combine($"D:\\Chennai\\DFS\\DFSFiles_Exported\\OneDimensional\\{org}\\OneDimOutputAllWS\\DFSInput\\DFS0\\Deterministic\\{frDate}\\{fRun}",
                            $"AllWSDFS_{org}_{frDate}_{fRun}_CtrlFile.dfs0");

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

        public string GenerateEnsembleDFS0(string frDate, string fRun, string org, string ensemblePercentages)
        {
            var tablename = "";

            if (org.Equals("ECMWF"))
            {
                tablename = "ECMWFFCastOneDimensionalEnsembleAllWS";
            }
            else if (org.Equals("NCEPGFS"))
            {
                tablename = "NCEPGFSFCastOneDimensionalEnsembleAllWS";
            }

            string columns = "\"NemamWS{0}\", \"SriperumbudurWS{0}\", \"PillaipakkamWS{0}\", \"GunduperumbeduWS{0}\", \"OrathurWS{0}\", " +
                "\"GuduvancheryWS{0}\", \"NandhivaramWS{0}\", \"AdanurWS{0}\", \"ManimangalamWS{0}\", \"MalaipattuWS{0}\", \"ChowdrikalWS{0}\", " +
                "\"ChembarambakkamWS{0}\", \"CooumBigTankWS{0}\", \"TiruvallurCWS{0}\", \"PutlurWS{0}\", \"AranvoyalWS{0}\", \"KannapanpalayamWS{0}\", " +
                "\"AmmapalliWS{0}\", \"NagariWS{0}\", \"AyyaneriWS{0}\", \"KodakkalWS{0}\", \"MahendravadiWS{0}\", \"KaveripakkamWS{0}\", \"KesavaramWS{0}\", " +
                "\"KallarWS{0}\", \"NandiWS{0}\", \"RamapuramWS{0}\", \"PalayanurWS{0}\", \"PoondiWS{0}\", \"TamaraipakkamWS{0}\", \"TiruvallurKWS{0}\", \"KattankalWS{0}\", " +
                "\"KaranodaiWS{0}\", \"AvadiWS{0}\", \"CholavaramWS{0}\", \"RedhillsWS{0}\", \"MahabalipuramWS{0}\", \"ManamathyWS{0}\", \"KondangiWS{0}\", \"ThiruporurWS{0}\", " +
                "\"ThaiyurWS{0}\", \"AgaramthenWS{0}\"";


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

                    string query = $"SELECT {string.Format(columns, percentage)} FROM public.\"{tablename}\" WHERE \"FRDate\" = '{frDate}' and \"FRun\" = '{fRun}' order by \"FDTime\"";

                    DataTable ECMWFWSData = new DataTable();
                    NpgsqlCommand cmd = new NpgsqlCommand(query, connection);
                    NpgsqlDataAdapter adp = new NpgsqlDataAdapter(cmd);
                    adp.Fill(ECMWFWSData);

                    if (ECMWFWSData.Rows.Count == 0)
                    {
                        return $"Data is not present for FRDate: {frDate} and FRun: {fRun}";
                    }
                    else
                    {
                        // Define the item names for ECMWF weather stations
                        string[] itemNames = { "NemamWS", "SriperumbudurWS", "PillaipakkamWS", "GunduperumbeduWS",
                                    "OrathurWS", "GuduvancheryWS", "NandhivaramWS", "AdanurWS", "ManimangalamWS",
                                    "MalaipattuWS", "ChowdrikalWS", "ChembarambakkamWS", "CooumBigTankWS", "TiruvallurCWS",
                                    "PutlurWS", "AranvoyalWS", "KannapanpalayamWS", "AmmapalliWS", "NagariWS", "AyyaneriWS",
                                    "KodakkalWS", "MahendravadiWS", "KaveripakkamWS", "KesavaramWS", "KallarWS", "NandiWS",
                                    "RamapuramWS", "PalayanurWS", "PoondiWS", "TamaraipakkamWS", "TiruvallurKWS", "KattankalWS",
                                    "KaranodaiWS", "AvadiWS", "CholavaramWS", "RedhillsWS", "MahabalipuramWS", "ManamathyWS",
                                    "KondangiWS", "ThiruporurWS", "ThaiyurWS", "AgaramthenWS"
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
                        double[] times = new double[ECMWFWSData.Rows.Count];
                        DateTime currentTime = dfsstarttime;

                        double[,] values = new double[ECMWFWSData.Rows.Count, 42];
                        for (int i = 0; i < ECMWFWSData.Rows.Count; i++)
                        {
                            for (int j = 0; j < 42; j++)
                            {
                                values[i, j] = Math.Round(Convert.ToDouble(ECMWFWSData.Rows[i][j]), 1);
                            }
                        }

                        DfsFactory factory = new DfsFactory();
                        DfsBuilder builder = DfsBuilder.Create("ECMWFCastData", "dfs Timeseries Bridge", 10000);

                        int nt = 72; // Number of time steps

                        // Set up file header
                        builder.SetDataType(1);
                        builder.SetGeographicalProjection(factory.CreateProjectionUndefined());
                        IDfsTemporalAxis temporalAxis = factory.CreateTemporalEqCalendarAxis(eumUnit.eumUminute, dfsstarttime, 0, 60);//nt,ECMWFWSData.Rows.Count
                        builder.SetTemporalAxis(temporalAxis);
                        builder.SetItemStatisticsType(StatType.RegularStat);

                        // Add dynamic items
                        for (int i = 0; i < itemNames.Length; i++)
                        {
                            DfsDynamicItemBuilder item = builder.CreateDynamicItemBuilder();
                            item.Set(itemNames[i] + percentage.ToString(), eumQuantity.Create(eumItem.eumIRainfall, eumUnit.eumUmillimeter), DfsSimpleType.Double);
                            item.SetValueType(DataValueType.Instantaneous);
                            item.SetAxis(factory.CreateAxisEqD0());
                            builder.AddDynamicItem(item.GetDynamicItemInfo());
                        }

                        string dfsfilename = Path.Combine($"D:\\Chennai\\DFS\\DFSFiles_Exported\\OneDimensional\\{org}\\OneDimOutputAllWS\\DFSInput\\DFS0\\Ensemble\\{frDate}\\{fRun}",
                            $"AllWSDFS_{org}_{frDate}_{fRun}_Ens_{percentage}.dfs0");

                        builder.CreateFile(dfsfilename);

                        IDfsFile file = builder.GetFile();

                        // Write data to file
                        Dfs0Util.WriteDfs0DataDouble(file, times, values);
                        file.Close();
                    }
                }
            }

            // Return JSON response with list of file paths
            return $"{Perctntages.Count} DFS0 files generated successfully";
        }

    }
}