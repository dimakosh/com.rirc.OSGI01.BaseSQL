package com.rirc.OSGI01.BaseSQL;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.Builder;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.osgi.service.component.annotations.Component;

import com.rirc.OSGI01.ConnPrms;
import com.rirc.OSGI01.KDCompField;
import com.rirc.OSGI01.KDCompMethod;
import com.rirc.OSGI01.KDCompRun;
import com.rirc.OSGI01.KDCompStepInfoPing;
import com.rirc.OSGI01.KDCompType;
import com.rirc.OSGI01.KDConnection;
import com.rirc.OSGI01.KDStr;
import com.rirc.OSGI01.KDTransact;

@Component
@KDCompType(project= "KDOSGIFBSQL", type= "FBAdmin", name= "Обновление базы данных и права пользователей (SYSDBA)", roles= "zkp_Base")
public class Web_CnvData implements KDCompMethod {
	
	private final static String cProjectSgn= "WZKP";

	@KDCompField(clientType= "ConnPrms")
	ConnPrms connPrms;
	
	@KDCompField(name= "Обновить, начиная с номера (задавать только для вновь созданной базы)")
	int zCnv;

	@KDCompStepInfoPing(name= "Информация по обновлению", width= 500, height= 300)
	Collection<String> info;

	@KDCompRun(name= "Обновить базу")
	String run() throws Exception {
		if (zCnv!=0) {
			try (Connection conn= KDConnection.get(connPrms); Statement stmt= conn.createStatement();) {
				stmt.executeUpdate(
					"CREATE TABLE CnvOk01 (Prj varchar(20) NOT NULL, El varchar(50) NOT NULL, "+
					"R1 varchar(50), R2 varchar(50), PRIMARY KEY (Prj,El))");
				
			} catch (SQLException ex) {
			}
		}
		
		try (Connection conn= KDConnection.get(connPrms, true);
			 Statement stmt= conn.createStatement();
			 PreparedStatement pstmt_CnvOk= conn.prepareStatement("UPDATE or INSERT INTO CnvOk01 (Prj,El,R1) VALUES (?,?,?)")) {
			
			pstmt_CnvOk.setString(1, cProjectSgn);

			userBeak();

			String BaseGUID;
			try (PreparedStatement pstmt= conn.prepareStatement(
				"SELECT buh_Yur.Name,zkp_LS.Prim,zkp_LS.RGB "+
				"FROM zkp_LS "+
				"INNER JOIN buh_Yur ON buh_Yur.Id=zkp_LS.Yur "+
				"WHERE zkp_LS.Id=-1")) {

				try (ResultSet rs= pstmt.executeQuery()) {
					rs.next();
					
					BaseGUID= rs.getNString(2);
				}				
			}

			userBeak();
			
			int lCnv;
			try (PreparedStatement pstmt= conn.prepareStatement(
				"SELECT R1 "+
				"FROM CnvOk01 "+
				"WHERE Prj=? and El=''")) {

				pstmt.setString(1, cProjectSgn);
				
				try (ResultSet rs= pstmt.executeQuery()) {
					if (rs.next()) lCnv= rs.getInt(1);
					else lCnv= 0;
				}
			}

			userBeak();
			
			if (lCnv==0) {
				if (zCnv==0) throw new Exception("Нет данных об обновлениях.");
				else lCnv= zCnv;
			} else {
				if (zCnv!=0) throw new Exception("Для этой базы нельзя задавать начальный номер обновления.");
			}
			
			info.add("Установленная версия: "+lCnv);

			HttpClient client= HttpClient.newHttpClient();
			
			cnv: for (;;) {
				userBeak();

				int cnv;
				String sql;
				{
					HttpRequest req= HttpRequest.newBuilder()
						.uri(URI.create("https://www.rirc.info/BDCnv002.aspx"))
						.timeout(Duration.ofMinutes(1))
						.header("ProjectSgn", "WZKP")
						.header("BaseGUID", BaseGUID)
						.header("PWD", "")
						//.header("BaseName", "")
						//.header("PointGUID", "")
						.header("LCnv", String.valueOf(lCnv))
						.GET()
						.build();
					
					Iterator<String> iterator= client.send(req, HttpResponse.BodyHandlers.ofLines()).body().iterator();
					
					cnv= (iterator.hasNext())? Integer.parseInt(iterator.next()):0;

					if (cnv==0) break cnv;

					StringBuilder sb= new StringBuilder();
					while (iterator.hasNext()) {
						sb.append(iterator.next());
						sb.append('\n');
					}
					sql= sb.toString();
				}

				info.add("Проводим: "+cnv);
				
				SQLException rex= null;

				try (KDTransact kdTran= new KDTransact(conn)) {
					stmt.executeUpdate(sql);

					pstmt_CnvOk.setString(2, "");
					pstmt_CnvOk.setString(3, String.valueOf(cnv));
					pstmt_CnvOk.executeUpdate();

					conn.commit();
					
					lCnv= cnv;
				} catch (SQLException ex) {
					rex= ex;
				}
				
				{
					Builder bldr= HttpRequest.newBuilder()
						.uri(URI.create("https://www.rirc.info/BDCnv002.aspx"))
						.timeout(Duration.ofMinutes(1))
						.header("ProjectSgn", "WZKP")
						.header("BaseGUID", BaseGUID)
						.header("PWD", "")
						//.header("BaseName", "")
						//.header("PointGUID", "")
						.header("Cnv", String.valueOf(cnv));
					
					if (rex!=null) bldr= bldr.header("Error", KDStr.getExMessage(rex));
					
					HttpRequest req= bldr
						.GET()
						.build();

					client.send(req, HttpResponse.BodyHandlers.discarding());
				}
				
				if (rex!=null) throw rex;
			}
			
			{
				Set<String> aTableName= new HashSet<String>();
				try (PreparedStatement pstmt= conn.prepareStatement("SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0")) {
					try (ResultSet rs= pstmt.executeQuery()) {
						while (rs.next()) aTableName.add(rs.getString(1).trim());
					}				
				}
				
				for (String cTableName : aTableName) {
					userBeak();
	
					info.add(cTableName);
	
					try {
						stmt.executeUpdate(
							"reCREATE TRIGGER "+cTableName+"_KodOperat_ODate FOR "+cTableName+"\n"+
							"ACTIVE BEFORE INSERT OR UPDATE POSITION 99\n"+
							"AS\n"+
							" DECLARE VARIABLE KodOperat smallint;\n"+
							"BEGIN\n"+
							" SELECT KodOperat FROM CurKodOperat INTO :KodOperat;\n"+
							" New.KodOperat= KodOperat;\n"+
							" New.ODate= CURRENT_TIMESTAMP;\n"+
							"END\n");
					} catch (SQLException ex) {
					}
					
					try {
						stmt.executeUpdate(
							"reCREATE TRIGGER "+cTableName+"_I_KodOperat_ODate FOR "+cTableName+"\n"+
							"ACTIVE BEFORE INSERT POSITION 99\n"+
							"AS\n"+
							" DECLARE VARIABLE KodOperat smallint;\n"+
							"BEGIN\n"+
							" SELECT KodOperat FROM CurKodOperat INTO :KodOperat;\n"+
							" New.IKodOperat= KodOperat;\n"+
							" New.IODate= CURRENT_TIMESTAMP;\n"+
							"END\n");
					} catch (SQLException ex) {
					}
				}
			}
			
			{
				String cSeansGUID;
				List<String> aModules= new ArrayList<String>();
				List<Boolean> aMFl= new ArrayList<Boolean>();
				
				{
					HttpRequest req= HttpRequest.newBuilder()
						.uri(URI.create("https://www.rirc.info/BDUpdate004.aspx"))
						.timeout(Duration.ofMinutes(1))
						.header("ProjectSgn", cProjectSgn)
						.header("BaseGUID", BaseGUID)
						.header("PWD", "")
						//.header("BaseName", "")
						//.header("PointGUID", "")
						.GET()
						.build();

					Iterator<String> iterator= client.send(req, HttpResponse.BodyHandlers.ofLines()).body().iterator();

					if (iterator.hasNext() && "ModulList004".equals(iterator.next())) {
						if (iterator.hasNext()) cSeansGUID= iterator.next();
						else throw new Exception("cSeansGUID");

						if (iterator.hasNext()) iterator.next();
						else throw new Exception("cAdm");
					} else throw new Exception("ModulList004");

					
					while (iterator.hasNext()) {
						String s= iterator.next();
						if (s.isEmpty()) break;
						aModules.add(s);
						aMFl.add(false);
					}

					while (iterator.hasNext()) {
						String s= iterator.next();
						if (s.isEmpty()) break;
						aModules.add(s);
						aMFl.add(true);
					}
				}

				info.add(cSeansGUID);

				try (PreparedStatement pstmt= conn.prepareStatement(
					"SELECT R1 "+
					"FROM CnvOk01 "+
					"WHERE Prj=? and El=?")) {

					pstmt.setString(1, cProjectSgn);

					mdls: for (int m= 0; m<aModules.size(); m++) {
						userBeak();

						String cSQLModulName= aModules.get(m);
						boolean mFl= aMFl.get(m);
						
						info.add(cSQLModulName);

						pstmt.setString(2, cSQLModulName);
						
						String cSQLModulGUID1;
						String cSQLModulGUID2;
						try (ResultSet rs= pstmt.executeQuery()) {
							cSQLModulGUID1= (rs.next())? cSQLModulGUID1= rs.getString(1):null;
						}
						if (cSQLModulGUID1==null) cSQLModulGUID1= "";
						
						List<String> aSQLText;
						{
							HttpRequest req= HttpRequest.newBuilder()
								.uri(URI.create("https://www.rirc.info/BDUpdate004.aspx"))
								.timeout(Duration.ofMinutes(1))
								.header("ProjectSgn", cProjectSgn)
								.header("BaseGUID", BaseGUID)
								.header("SQLModulName", cSQLModulName)
								.header("SQLModulGUID", cSQLModulGUID1)
								.header("SeansGUID", cSeansGUID)
								.GET()
								.build();

							Iterator<String> iterator= client.send(req, HttpResponse.BodyHandlers.ofLines()).body().iterator();

							if (iterator.hasNext() && "SQLModul004".equals(iterator.next())) {
								if (iterator.hasNext()) cSQLModulGUID2= iterator.next();
								else throw new Exception("cSQLModulGUID2");
							} else throw new Exception("SQLModul004");
							
							if (!mFl && KDStr.equals(cSQLModulGUID1, cSQLModulGUID2)) {
								info.add("Загружен ранее.");
								continue mdls;
							}
							
							aSQLText= new ArrayList<String>();
							while (iterator.hasNext()) aSQLText.add(iterator.next()); 
						}
						
						Map<String,String> mErr= new HashMap<String,String>();
						for (int t= 0; t< aSQLText.size(); t++) {
							userBeak();

							info.add(cSQLModulName+' '+String.valueOf(t));
							
							String sqlText= aSQLText.get(t);

							HttpRequest req= HttpRequest.newBuilder()
								.uri(URI.create("https://www.rirc.info/BDUpdate004.aspx"))
								.timeout(Duration.ofMinutes(1))
								.header("ProjectSgn", cProjectSgn)
								.header("BaseGUID", BaseGUID)
								.header("SQLModulName", cSQLModulName)
								.header("SQLModulGUID", cSQLModulGUID1)
								.header("SeansGUID", cSeansGUID)
								.header("SQLText", sqlText)
								.GET()
								.build();
							
							Iterator<String> iterator= client.send(req, HttpResponse.BodyHandlers.ofLines()).body().iterator();

							String sql;
							if (iterator.hasNext() && "SQLText004".equals(iterator.next())) {
								StringBuilder sb= new StringBuilder();
								while (iterator.hasNext()) {
									String s= iterator.next();
									
									s= s.replace("CREATE PROCEDURE", "CREATE or ALTER PROCEDURE");
									s= s.replace("CREATE TRIGGER", "CREATE or ALTER TRIGGER");
									
									sb.append(s);
									sb.append('\n');
								}
								sql= sb.toString();
								
							} else throw new Exception("SQLText004");
							
							try {
								stmt.executeUpdate(sql);
							} catch (SQLException ex) {
								String err= KDStr.getExMessage(ex);
								info.add(err);
								mErr.put(sqlText, err);
							}							
						}
						
						info.add("Ошибок: "+mErr.size());
						info.add("Загружен: "+cSQLModulName);
						
						if (mErr.isEmpty()) {
							pstmt_CnvOk.setString(2, cSQLModulName);
							pstmt_CnvOk.setString(3, cSQLModulGUID2);
							pstmt_CnvOk.executeUpdate();
						}
						
						{
							Builder bldr= HttpRequest.newBuilder()
								.uri(URI.create("https://www.rirc.info/BDUpdate004.aspx"))
								.timeout(Duration.ofMinutes(1))
								.header("SQLModulGUID", cSQLModulGUID2)
								.header("SeansGUID", cSeansGUID);

							for (Entry<String,String> e : mErr.entrySet()) bldr= bldr.header("Result_"+e.getKey(), e.getValue());
							
							HttpRequest req= bldr
								.GET()
								.build();

							Iterator<String> iterator= client.send(req, HttpResponse.BodyHandlers.ofLines()).body().iterator();
							
							if (!iterator.hasNext() || !"ModulResult004".equals(iterator.next()))
								info.add("Ошибка: Возврата результата.");
						}
					}
				}
			}
			
			{
				info.add("Права пользователей");
				List<String> aAllUsers= new ArrayList<String>();
				try (PreparedStatement pstmt= conn.prepareStatement("SELECT distinct RDB$USER FROM RDB$USER_PRIVILEGES WHERE RDB$USER not in ('SYSDBA','PUBLIC') and RDB$USER_TYPE=8")) {
					try (ResultSet rs= pstmt.executeQuery()) {
						while (rs.next()) aAllUsers.add(rs.getString(1));
					}				
				}
				info.add("Пользователей: "+aAllUsers.size());
				
				List<String> aChangeUsers= aAllUsers;

				info.add("DeleteAllUserRolePrivileges");

				try (PreparedStatement pstmt= conn.prepareStatement("SELECT UserName, lOk FROM DeleteAllUserRolePrivileges")) {
					try (ResultSet rs= pstmt.executeQuery()) {
						while (rs.next()) {
							userBeak();

							String cUSER= rs.getString(1);
							short lOk= rs.getShort(2);
							info.add(cUSER+' '+((lOk!=0)? "обработан":"ошибка"));
						}
					}				
				}

				try (PreparedStatement pstmt= conn.prepareStatement("Execute procedure ReActivateUserRolePrivileges ?")) {
					for (String cUSER : aChangeUsers) {
						userBeak();

						info.add("Даём права: "+cUSER);
						
						pstmt.setString(1, cUSER);
						pstmt.executeUpdate();

						info.add("Даны права: "+cUSER);
					}
				}

				userBeak();

				info.add("GrantRightToProcedureAndTrigger");
				stmt.executeUpdate("Execute procedure GrantRightToProcedureAndTrigger");

				userBeak();

				try {
					stmt.executeUpdate("GRANT CREATE PROCEDURE TO public");
				} catch (SQLException ex) {
					info.add(ex.toString());
				}
			}

			return "Завершено: "+lCnv;
		}
	}
}
