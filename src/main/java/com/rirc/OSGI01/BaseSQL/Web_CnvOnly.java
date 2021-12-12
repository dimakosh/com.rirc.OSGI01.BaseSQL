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
import java.util.Collection;
import java.util.Iterator;

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
@KDCompType(project= "KDOSGIFBSQL", type= "FBAdmin", name= "Только проведение cnv (SYSDBA)", roles= "zkp_Base")
public class Web_CnvOnly implements KDCompMethod {
	
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

			return "Завершено: "+lCnv;
		}
	}
}
