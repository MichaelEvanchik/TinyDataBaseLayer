using System;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Data;

namespace TinyDataBaseLayer
{
    public class clsDatabaseLayer
    {
        SqlConnection sconn;
        SqlDataAdapter sda;
        SqlCommand scmd;

        OleDbConnection oconn;
        OleDbCommand ocmd;
        OleDbDataAdapter oda;


        string slConnectionString = "";
        int ilConnectionType = 0;
        int iTimeOut = 30000;//30 seconds
        
        
        public clsDatabaseLayer(string sConnectionString,int iConnectionType)
        {
            slConnectionString = sConnectionString;
            ilConnectionType = iConnectionType;
        }

        public clsDatabaseLayer(string sConnectionString,int iConnectionType, int icmdTimeoutMilliseconds)
        {
            slConnectionString = sConnectionString;
            ilConnectionType = iConnectionType;
            iTimeOut = icmdTimeoutMilliseconds;
        }

        public int Timeout
        {
            get
            {
                return this.iTimeOut;
            }
            set
            {
                this.iTimeOut = value;
            }
        }
 
        bool IsEmpty(DataSet dataSet)
        {
            foreach (DataTable table in dataSet.Tables)
                if (table.Rows.Count != 0) return false;

            return true;
        }

        public string DbConnect()
        {
            //return evaluated as bool
            //if not valid bool exception inside
                try
                {
                    switch (ilConnectionType)
                    {
                        case 1:
                            sconn = new SqlConnection();
                            sconn.ConnectionString = slConnectionString;
                            sconn.Open();
                            return "1";
                        case 2:
                            oconn = new OleDbConnection();
                            oconn.ConnectionString = slConnectionString;
                            oconn.Open();
                            return "1";
                        default:
                            return "0";
                    }
                }
                catch(Exception ex)
                {
                    return ex.Message + " " + ex.InnerException.Message;
                }
            
        }

        public string DbClose()
        {
            switch (ilConnectionType)
            {
                case 1:
                    try
                    {
                        sconn.Close();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message.ToString() + " " + ex.InnerException.Message;
                    }
                    return "1";
               

                case 2:
                    try
                    {
                        oconn.Close();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message.ToString() + " " + ex.InnerException.Message;
                    }
                    return "1";

                default:
                    return "0";

            }
       
        }

        public string DbScalar(string sSql)
        {
            object oret = null;
            switch (ilConnectionType)
            {
                case 1:
                    
                    try
                    {
                        scmd = new SqlCommand(sSql,sconn);
                        scmd.CommandTimeout = iTimeOut;
                        scmd.CommandText = sSql;
                        oret = scmd.ExecuteScalar();
                        return oret.ToString();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message.ToString() + " " + ex.InnerException.Message;
                    }
                

                case 2:
                    try
                    {
                        ocmd = new OleDbCommand(sSql,oconn);
                        ocmd.CommandTimeout = iTimeOut;
                        ocmd.CommandText = sSql;
                        oret = ocmd.ExecuteScalar();
                        return oret.ToString();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message.ToString() + " " + ex.InnerException.Message;
                    }
                   

                default:
                    return "bad database code";

            }
           
        }

        public DataSet DbDataSet(string sSql)
        {
            DataSet ds = new DataSet();
            switch (ilConnectionType)
            {
                case 1:
                    try
                    {
                        scmd = new SqlCommand(sSql,sconn);
                        scmd.CommandTimeout = iTimeOut;
                        sda = new SqlDataAdapter(); 
                        scmd.CommandText = sSql;
                        sda = new SqlDataAdapter(sSql, sconn); 
                        sda.Fill(ds, "details"); 
                        return ds;
                    }
                    catch (Exception ex)
                    {
                        return ds;
                    }
              

                case 2:
                    try
                    {
                        ocmd = new OleDbCommand(sSql, oconn);
                        ocmd.CommandTimeout = iTimeOut;
                        oda = new OleDbDataAdapter(); 
                        ocmd.CommandText = sSql;
                        oda = new OleDbDataAdapter(sSql, oconn); 
                        oda.Fill(ds, "details"); 
                        return ds;
                    }
                    catch (Exception ex)
                    {
                        return ds;
                    }
              

                default:
                    return ds;
            
        }
            return ds;
    }

        public DataTable DbDataTable(string sSql)
        {
            DataSet ds = new DataSet();
           
            switch (ilConnectionType)
            {
                case 1:
                    try
                    {
                        scmd = new SqlCommand(sSql, sconn);
                        scmd.CommandTimeout = iTimeOut;
                        sda = new SqlDataAdapter();
                        scmd.CommandText = sSql;
                        sda = new SqlDataAdapter(sSql, sconn);
                        sda.Fill(ds, "details");
                        return ds.Tables[0];
                    }
                    catch (Exception ex)
                    {
                        return ds.Tables[0];
                    }


                case 2:
                    try
                    {
                        ocmd = new OleDbCommand(sSql, oconn);
                        ocmd.CommandTimeout = iTimeOut;
                        oda = new OleDbDataAdapter();
                        ocmd.CommandText = sSql;
                        oda = new OleDbDataAdapter(sSql, oconn);
                        oda.Fill(ds, "details");
                        return ds.Tables[0];
                    }
                    catch (Exception ex)
                    {
                        return ds.Tables[0];
                    }


                default:
                    return ds.Tables[0];

            }
          
        }

        public string DbExecute(string sSql)
        {

            int iret = -1;
            switch (ilConnectionType)
            {
                case 1:

                    try
                    {
                        scmd = new SqlCommand(sSql,sconn);
                        scmd.CommandTimeout = iTimeOut;
                        iret = scmd.ExecuteNonQuery();
                        return iret.ToString();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message.ToString() + " " + ex.InnerException.Message;
                    }
                    

                case 2:
                    try
                    {
                        ocmd = new OleDbCommand(sSql, oconn);
                        ocmd.CommandTimeout = iTimeOut;
                        ocmd.CommandText = sSql;
                        iret = ocmd.ExecuteNonQuery();
                        return iret.ToString();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message.ToString() + " " + ex.InnerException.Message;
                    }
                  

                default:
                    return "bad database code";

            }
    
        }
    }
 }

