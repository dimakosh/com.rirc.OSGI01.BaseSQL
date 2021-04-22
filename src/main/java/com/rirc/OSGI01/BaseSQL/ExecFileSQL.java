package com.rirc.OSGI01.BaseSQL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.osgi.service.component.annotations.Component;

import com.rirc.OSGI01.ConnPrms;
import com.rirc.OSGI01.KDCompField;
import com.rirc.OSGI01.KDCompMethod;
import com.rirc.OSGI01.KDCompRun;
import com.rirc.OSGI01.KDCompStepInfoPing;
import com.rirc.OSGI01.KDCompType;
import com.rirc.OSGI01.KDConnection;
import com.rirc.OSGI01.KDStr;
import com.rirc.OSGI01.KDTime;

@Component
@KDCompType(project= "KDOSGIFBSQL", type= "FBAdmin", name= "Выполнить SQL (из файла)", roles= "zkp_Base")
public class ExecFileSQL implements KDCompMethod {

	@KDCompField(clientType= "ConnPrms")
	ConnPrms connPrms;

	@KDCompField(name= "Файл", clientType= "FileUpload", isRequired= true)
	File file;

	@KDCompStepInfoPing(name= "Информация по выполнению", width= 500, height= 200)
	Collection<String> info;
	
	@KDCompRun(name= "Выполнить SQL (из файла)")
	String run() throws Exception {
		if (file==null) throw new Exception("Нет загруженных файлов");

		boolean lAddTrig= false;
		try (Connection conn= KDConnection.get(connPrms, true);
			 BufferedReader inp= new BufferedReader(new InputStreamReader(new FileInputStream(file), "Cp1251"))) {
			
			String BLOCK_MONTH= String.valueOf(KDTime.getCurMonth());

			String cFstLine= null;
			StringBuilder cCommand= new StringBuilder();
			String str;
			int okCnt= 0;
			int errCnt= 0;
			StringBuilder errMess= new StringBuilder();
			while ((str= inp.readLine())!=null) {
				userBeak();

				if (str.startsWith("*") && !str.startsWith("*/")) continue;
				str= str.stripTrailing();
				
				str= str.replace("CREATE PROCEDURE", "CREATE or ALTER PROCEDURE");
				str= str.replace("CREATE TRIGGER", "CREATE or ALTER TRIGGER");

				str= str.replace("__KD_BLOCK_MONTH__", BLOCK_MONTH);

				String cGO= str.trim().toUpperCase();
				
				if (lAddTrig) lAddTrig= cGO.contains("CREATE") && cGO.contains("TABLE");
				
				if ("GO".equals(cGO)) {
					
					if (cCommand.length()>0) {
						List<String> aSQL= new ArrayList<String>();
						
						String cmd= cCommand.toString();

						if (cmd.charAt(0)=='%') {
							throw new Exception(cmd);
						} else
							aSQL.add(cmd);
						
						for (String cCmd : aSQL) {
							info.add("Ok "+okCnt+" Err "+errCnt);
							try (Statement stmt= conn.createStatement()) {
								stmt.executeUpdate(cCmd);
								okCnt++;
							} catch (SQLException ex) {
								String m= cFstLine+'\n'+KDStr.getExMessage(ex);
								
								errCnt++;
								errMess.append(m);
								errMess.append('\n');
								
								info.add(m);
							}
						}
						
						cFstLine= null;
						cCommand.setLength(0);;
					}
				} else {
					if (cCommand.length()>0 || !str.isEmpty()) {
						if (cFstLine==null) {
							cFstLine= str;
						}
						
						cCommand.append(str);
						cCommand.append('\n');
					}
				}
			}

			try (Statement stmt= conn.createStatement()) {
				stmt.executeUpdate("UPDATE RDB$PROCEDURES SET RDB$PROCEDURE_SOURCE= null WHERE RDB$PROCEDURE_SOURCE is not null and (RDB$SYSTEM_FLAG is null or RDB$SYSTEM_FLAG=0)");
			}
			try (Statement stmt= conn.createStatement()) {
				stmt.executeUpdate("UPDATE RDB$TRIGGERS SET RDB$TRIGGER_SOURCE= null WHERE RDB$TRIGGER_SOURCE is not null and (RDB$SYSTEM_FLAG is null or RDB$SYSTEM_FLAG=0) and SubStr(RDB$TRIGGER_NAME,4,4)!='$'");
			}

			return "Ok "+okCnt+" Err "+errCnt+'\n'+ errMess;
		} catch (Exception ex) {
			return KDStr.getExMessage(ex);
		}
	}	
}
