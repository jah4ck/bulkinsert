using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace bulkinsert
{
    class Program
    {
        private static char Separateur { get; set; }
        public static int EnteteIndex { get; set; }
        static void Main(string[] args)
        {
            string path = @"W:\TempDoc\global_import.csv";
            Separateur = ';';
            EnteteIndex = 0;
            string nameDataTable = "donnee.dbo.corri_april_2018";
            string connString = @"Server=10.0.75.1,1433;Uid=sa;Pwd=Ascpkn16!?;";
            string[] ligneFichier = File.ReadAllLines(path);


            //détail de la première ligne
            string[] colonneFirstLigne = ligneFichier[1].Split(';');

            //nombre de colonne à créer
            string[] detailColonne = new string[colonneFirstLigne.Length];

            Console.WriteLine("Création datatable");
            DataTable data = GetDataSourceFromFile(path);
            DataTable dataModif = SetType(data);
            Console.WriteLine("Création de la table");
            string script = CreateTABLE(nameDataTable, dataModif);
            Console.WriteLine(script);
            
            Console.WriteLine("debut sql");
            using (SqlConnection connection = new SqlConnection(connString))
            {
                
                // make sure to enable triggers
                // more on triggers in next post
                SqlBulkCopy bulkCopy =
                    new SqlBulkCopy
                    (
                        connection,
                        SqlBulkCopyOptions.TableLock |
                        SqlBulkCopyOptions.FireTriggers |
                        SqlBulkCopyOptions.UseInternalTransaction,
                        null
                    )
                    {

                        // set the destination table name
                        DestinationTableName = nameDataTable
                    };
                Console.WriteLine("ouverture de connexion");
                connection.Open();


                SqlCommand cmd = new SqlCommand(script);
                cmd.Connection = connection;
                cmd.ExecuteNonQuery();

                // write the data in the "dataTable"
                Console.WriteLine("debut copie");
                bulkCopy.WriteToServer(data);
                Console.WriteLine("fermeture de connexion");
                connection.Close();

            }

            // reset
            Console.WriteLine("clear datatable");
            data.Clear();

        }




        public static string CreateTABLE(string tableName, DataTable table)
        {
            string sqlsc;
            sqlsc = "CREATE TABLE " + tableName + "(";
            for (int i = 0; i < table.Columns.Count; i++)
            {
                sqlsc += "\n [" + table.Columns[i].ColumnName + "] ";
                string columnType = table.Columns[i].DataType.ToString();
                //Console.WriteLine(columnType);
                switch (columnType)
                {
                    case "System.Int32":
                        sqlsc += " int ";
                        break;
                    case "System.Int64":
                        sqlsc += " bigint ";
                        break;
                    case "System.Int16":
                        sqlsc += " smallint";
                        break;
                    case "System.Byte":
                        sqlsc += " tinyint";
                        break;
                    case "System.Decimal":
                        sqlsc += " decimal(9,2) ";
                        break;
                    case "System.DateTime":
                        sqlsc += " datetime ";
                        break;
                    case "System.String":
                    default:
                        sqlsc += string.Format(" nvarchar({0}) ", table.Columns[i].MaxLength == -1 ? "max" : table.Columns[i].MaxLength.ToString());
                        break;
                }
                if (table.Columns[i].AutoIncrement)
                    sqlsc += " IDENTITY(" + table.Columns[i].AutoIncrementSeed.ToString() + "," + table.Columns[i].AutoIncrementStep.ToString() + ") ";
                if (!table.Columns[i].AllowDBNull)
                    sqlsc += " NOT NULL ";
                sqlsc += ",";
            }
            return sqlsc.Substring(0, sqlsc.Length - 1) + "\n)";
        }




        public static DataTable GetDataSourceFromFile(string fileName)
        {
            DataTable dt = new DataTable("CreditCards");
            string[] columns = null;

            var lines = File.ReadAllLines(fileName);

            // assuming the first row contains the columns information
            if (lines.Count() > 0)
            {
                columns = lines[EnteteIndex].Split(new char[] { Separateur });

                foreach (var column in columns)
                    dt.Columns.Add(column);
            }

            // reading rest of the data
            for (int i = EnteteIndex+1; i < lines.Count(); i++)
            {
                DataRow dr = dt.NewRow();
                string[] values = lines[i].Split(new char[] { Separateur });

                for (int j = 0; j < values.Count() && j < columns.Count(); j++)
                {
                    dr[j] = values[j];
                    //Console.WriteLine($"valeur = {dr[j]} type = {dr[j].GetType()}");
                }

                dt.Rows.Add(dr);
            }
            return dt;
        }

        public static DataTable SetType(DataTable table)
        {
            Console.WriteLine(table.Columns.Count);
            DataTable dtCloned = table.Clone();
            for (int i = 0; i < table.Columns.Count; i++)//chaque colonne
            {
                string[] column = new string[table.Rows.Count];
                for (int j = 0; j < table.Rows.Count; j++)//chaque ligne
                {
                    column[j] = table.Rows[j][i].ToString();
                }
                //return the column's data type
                string dataType = GetType(column);

                //traitement pour remplacer le "." par "," pour les déciamls
                if (dataType == "System.Decimal")
                {
                    for (int j = 0; j < table.Rows.Count; j++)//chaque ligne
                    {
                        table.Rows[j][i]= table.Rows[j][i].ToString() != string.Empty ? table.Rows[j][i].ToString().Replace('.',',') : null;
                    }
                }
                if (dataType == "System.Int32" || dataType == "System.Int64" || dataType == "System.DateTime")
                {
                    for (int j = 0; j < table.Rows.Count; j++)//chaque ligne
                    {
                        table.Rows[j][i] = table.Rows[j][i].ToString() != string.Empty ? table.Rows[j][i].ToString() :  null;
                        table.Rows[j][i] = table.Rows[j][i].ToString().ToUpper() == "NULL" ? null : table.Rows[j][i].ToString().ToLower();
                    }
                }
                

                Console.WriteLine($"colonne {i} = {dataType}");
                dtCloned.Columns[i].DataType = System.Type.GetType(dataType);

            }
            foreach (DataRow row in table.Rows)
            {
                dtCloned.ImportRow(row);
            }
            return dtCloned;
            
            
        }

        public static string GetType(string[] column)
        {
            int countBit = 0;
            int countDateTime = 0;
            int countDecimal = 0;
            int countInt = 0;
            int countInt64 = 0;
            int countString = 0;
            string retour = string.Empty;
            foreach (var item in column)
            {
                if (item != null && item.ToUpper() != "NULL" && !string.IsNullOrEmpty(item))
                {
                    bool _bit = Boolean.TryParse(item, out bool val1);
                    bool _dateTime = DateTime.TryParse(item, out DateTime val2);
                    bool _decimal = Decimal.TryParse(item.Replace('.',','), out Decimal val3);
                    bool _int = Int32.TryParse(item, out Int32 val4);
                    bool _int64 = Int64.TryParse(item, out Int64 val5);
                    if (_int)
                    {
                        countInt++;
                    }
                    else if(_int64)
                    {
                        countInt64++;
                    }
                    else if (_decimal)
                    {
                        countDecimal++;
                    }
                    else if (_dateTime)
                    {
                        countDateTime++;
                    }
                    else if (_bit)
                    {
                        countBit++;
                    }
                    else
                    {
                        countString++;
                    }
                }
            }
            if (countBit > 0 && countDateTime == 0 && countDecimal == 0 && countInt == 0 && countString == 0 && countInt64 == 0)
            {
                retour = "System.Boolean";
            }
            else if (countBit == 0 && countDateTime > 0 && countDecimal == 0 && countInt == 0 && countString == 0 && countInt64 == 0)
            {
                retour = "System.DateTime";
            }
            else if (countBit == 0 && countDateTime == 0 && countDecimal > 0 && countInt == 0 && countString == 0 && countInt64 == 0)
            {
                retour = "System.Decimal";
            }
            else if (countBit == 0 && countDateTime == 0 && countDecimal == 0 && countInt > 0 && countString == 0 && countInt64 == 0)
            {
                retour = "System.Int32";
            }
            else if (countBit == 0 && countDateTime == 0 && countDecimal == 0 && countInt == 0 && countString > 0 && countInt64 == 0)
            {
                retour = "System.String";
            }
            else if (countBit == 0 && countDateTime == 0 && countDecimal == 0 && countInt == 0 && countString == 0 && countInt64 > 0)
            {
                retour = "System.Int64";
            }
            else
            {
                retour = "System.String";
            }
            return retour;



        }
    }
}
