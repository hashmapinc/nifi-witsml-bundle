/**
 * 
 */
package com.hashmap.concho.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import com.hashmap.concho.util.HDFSUtility;

/**
 * @author miteshrathore
 *
 */
public class TimeLogParsing {

	private static final Properties mnemonicProperty = readPropertyFile();

	// 636 +1 header
	private static final String[] FILE_HEADER = { "WELLID", "LOGTIME", "MESSAGE", "3GAS", "ABI", "ACET", "ADBBP",
			"ADBKP", "ADBLP", "ADBLV", "ADDP", "ADDRM", "ADDRT", "ADRRT", "ADRSP", "ADS", "ADSE", "ADT", "ADTHR",
			"ADTPD", "ADTRT", "ADTSP", "ADWOB", "ADWRT", "AHRS", "AIRPRESS", "AMAPM", "AMPHRS", "AMPI1", "ANNP", "ANP",
			"ARPM", "AVGWPDA", "AXIALVB", "AZ", "BALR", "BDEP", "BHT", "BKSP", "BLKT", "BLP", "BR", "BRTG", "BTCL",
			"BTEM", "C1", "C10", "C100", "C17", "C1M", "C2", "C2M", "C3", "C31", "C34", "C3M", "C4", "C5", "C50", "C51",
			"C52", "C53", "C54", "C55", "C56", "C57", "C58", "C59", "C60", "C61", "C62", "C63", "C64", "C65", "C66",
			"C67", "C68", "C69", "C70", "C71", "C72", "C73", "C74", "C75", "C76", "C77", "C78", "C79", "C80", "C81",
			"C82", "C83", "C84", "C85", "C86", "C87", "C88", "C89", "C90", "C91", "C92", "C93", "C94", "C95", "C96",
			"C97", "C98", "C99", "CAHTT", "CASING", "CATCT", "CAZI", "CDATE", "CFT", "CHARR", "CHKWD", "CHRTG", "CINC",
			"CIRH", "CO2", "CONO", "CONTINC", "CONTINUO", "CPOS", "CPP", "CSV", "CTIME", "CTOR", "CTSV", "CTV1", "CTV2",
			"CTV3", "CTV4", "CTV5", "CTV6", "CTV7", "CTV8", "CUSTOMAN", "CUSTOMFL", "CUSTOMMU", "CWR", "DADOC", "DAGE",
			"DALS", "DAMSE", "DAMU", "DAOBJ", "DARDP", "DAROP", "DARR", "DARW", "DASCR", "DATM", "DATSE", "DBRMN",
			"DBRMX", "DDPSP", "DEFLECTI", "DEN", "DENSITYB", "DENSITYD", "DEPT", "DEXP", "DFPTG", "DH1", "DH2", "DH3",
			"DH4", "DHP1", "DHT", "DHWOB", "DIFP", "DIN", "DOC", "DOUT", "DRACT", "DRROP", "DSSAL", "DTDSP", "DTEMP",
			"DTF", "DWDV", "DWOB", "DYNAZ", "DYNIN", "ECD", "EDRFLOW", "EDRFLOWP", "ELPOS", "ENO", "EXFL", "EXG1",
			"EXG2", "F1GLT", "FDD", "FDEN", "FDENB", "FEHA", "FELA", "FEST", "FESTE", "FGLAL", "FL1GL", "FLOW", "FLOWC",
			"FLOWLINE", "FLOWLINEP", "FSTK", "G1AFTERC", "G1BATTER", "G1BOOSTP", "G1COOLAN", "G1CRANKC", "G1ENGINE",
			"G1FUELCO", "G1FUELFI", "G1FUELPR", "G1LEFTEX", "G1LOAD", "G1OILFIL", "G1OILPRE", "G1POWERD", "G1RIGHTE",
			"G1RPM", "G2AFTERC", "G2BATTER", "G2BOOSTP", "G2COOLAN", "G2CRANKC", "G2ENGINE", "G2FUELCO", "G2FUELFI",
			"G2FUELPR", "G2LEFTEX", "G2LOAD", "G2OILFIL", "G2OILPRE", "G2POWERD", "G2RIGHTE", "G2RPM", "G3AFTERC",
			"G3BATTER", "G3BOOSTP", "G3COOLAN", "G3CRANKC", "G3ENGINE", "G3FUELCO", "G3FUELFI", "G3FUELPR", "G3LEFTEX",
			"G3LOAD", "G3OILFIL", "G3OILPRE", "G3POWERD", "G3RIGHTE", "G3RPM", "GABHD", "GABKD", "GACAP", "GACHD",
			"GAHTD", "GAINLOSS", "GAM", "GAMFR", "GAMMA082", "GAMMA2", "GAMMAAZD", "GAMMAAZL", "GAMMAAZU", "GAMMAMWD",
			"GAST1", "GAST2", "GATMC", "GATMT", "GBFLOW", "GDEP", "GRC", "GTF", "GTFFR", "GTS", "GV0", "GV1", "GV10",
			"GV2", "GV3", "GV4", "GV5", "GV6", "GV7", "GV8", "GV9", "H2ON", "H2OO", "H2S", "HAMPPROP", "HCF", "HDIAM",
			"HL", "HOLXY", "HOLZ", "HVYRT", "IC4", "IC5", "IC6", "INCL", "INCWD", "INCWHILE", "INDP3", "INDP4", "INDP5",
			"INFLE", "INPD2", "INPDT", "IROP", "LATVB", "LWEAR", "MAINSUCK", "MAKEUPTON", "MAKEUPTOR", "MASTERCL",
			"MCTOR", "MDEN", "MGLAL", "MHKL", "MMDIF", "MMOTO", "MMTOR", "MPH", "MPRS", "MRPM", "MSE", "MSETG", "MTF",
			"MTFFR", "MTIN", "MTOR", "MTOU", "MTRPM", "MTTSL", "MTTST", "MTWOB", "MUDCL", "MV", "MVIS", "MVTT", "MVTT1",
			"MVTT2", "MWDCG", "MWDC_AZM", "MWDC_INC", "MWDDH", "MWDDIPA", "MWDGRAV", "MWDLP", "MWDLS", "MWDMAGF",
			"MWDPS", "MWDQU", "MWDRE", "MWDSHOCK", "MWDSS", "MWDSU", "MWDTEMP", "MWDVIB", "MWOB", "MWRPM", "N2P", "N2R",
			"N2VI", "N2VO", "NC4", "NEC5", "NRC5", "NRC6", "O2", "OBH", "OBR", "OBRTG", "OROP", "OVRP", "P1D", "P1RAT",
			"P2D", "P2RAT", "P3RAT", "P4RAT", "PAFL", "PAFS", "PAHT", "PC1G", "PC2G", "PC3G", "PC4G", "PC5G", "PCAS",
			"PCO2G", "PDAZIM", "PDINC", "PDRY", "PEST", "PESTE", "PFS", "PGAS", "PGAS2", "PHGP", "PIC4G", "PLAD",
			"PMGL", "PMGLT", "PNC4G", "POR1", "POR1B", "POR2", "POR2B", "PPSE", "PRAP", "PRESSURE", "PRESSURE1",
			"PRESSURE2", "PREST", "PSFL", "PTGN", "PVEHA", "PVELA", "PVLES", "PVOHA", "PVOLA", "PWDANNPR", "PWDINTPR",
			"R1D", "R2D", "RATE1", "RATE2", "RES1", "RES1B", "RES2", "RES2B", "REVERSED", "RINC", "RLNC", "RMSE",
			"ROLLINGI", "ROP", "ROT", "ROTARYCA", "RPM", "RPMTG", "RVIS", "SDAC", "SDEN", "SDEP", "SEVXY", "SEVZX",
			"SFLOW", "SGG", "SHK1", "SHOCK", "SHOCKAMP", "SHOCKAXIL", "SHOCKAXIM", "SHOCKAXL", "SHOCKLATL", "SHOCKLATM",
			"SHOCKRIS", "SHOCKRISD", "SHOCKTOT", "SKTTL", "SLIPSTICI", "SLIPSTICR", "SLURRYTT", "SMJ", "SNDEP",
			"SPCONTAZ", "SPCONTIN", "SPGAMMA", "SPGTF", "SPM1", "SPM2", "SPM3", "SPM4", "SPMTF", "SPP", "SPTEMP",
			"SRATE", "SRS", "SSI", "SSSI", "STATL", "STATU", "STICKSLID", "STICKSLIR", "STROKES1", "STROKES2",
			"STROKES3", "STT2L", "STT2U", "T1ST", "TANK1", "TANK2", "TANK3", "TANK4", "TANK5", "TANK6", "TANK7",
			"TANK8", "TANKS", "TC_1", "TC_2", "TC_3", "TC_4", "TC_5", "TC_6", "TC_7", "TC_8", "TC_9", "TDLTL", "TDPSP",
			"TDROT", "TDRST", "TDRTL", "TDSSP", "TDTOR", "TEHA", "TELA", "TEMP", "TEMPERAT", "TF", "TFIL", "TGAS",
			"TGR", "TGSANALY", "TONGPULL", "TOOLFACE", "TOOLREVE", "TOOLRPM", "TOP", "TOPDRIVER", "TOPDRIVET", "TOR",
			"TORQUECA", "TORTG", "TOTALMUD", "TOTALSTR", "TOTCORPM", "TOTCOTOR", "TPD", "TPO", "TPOTG", "TRIP1",
			"TRIP2", "TRIPTOTAH", "TRIPTOTAL", "TRPT", "TRSP", "TS1", "TS14", "TS2", "TS3", "TS4", "TT1AL", "TT1HI",
			"TT1LO", "TT2AL", "TT2HI", "TT2LO", "TTACC", "TTC_1", "TTC_2", "TTE", "TTEE", "TTFIL", "TTMSE", "TTOHA",
			"TTOLA", "TTORQ", "TTSL", "TTST", "TTWOB", "TV1", "TV10", "TV2", "TV3", "TV4", "TV5", "TV6", "TV7", "TV8",
			"TV9", "TVD", "TVDV", "UBFP", "UBFT", "VALPRESS", "VBCNT", "VBXYG", "VBXYZ", "VBZG", "VIB1", "VIBE", "VTGL",
			"VXYZG", "WDDIR", "WDSP", "WELLHEAD", "WETR", "WGAS", "WGASP", "WIT1", "WIT10", "WIT11", "WIT12", "WIT13",
			"WIT17", "WIT18", "WIT2", "WIT26", "WIT3", "WIT4", "WIT45", "WIT46", "WIT5", "WIT6", "WIT7", "WIT8", "WIT9",
			"WLGD", "WOB", "WOBSP", "WOBTG", "WPDA", "XY", "XYSHOCK", "XYVIBE", "Z", "ZSHOCK", "ZVIBE" };

	public void parseTimeLogData(String srcFilePath, int fileNumber) {

		// public static void main(String[] args) {

		PrintWriter depthLogPW = null;
		String parseFile = null;
		String fileName = null;
		File sourceFile = null;

		String hdfsTimeLoc = "/data_lake/pason/timelog/";
		String hdfsTimeLoc1 = "/data_lake/pason/timelog/1/";
		String hdfsTimeLoc2 = "/data_lake/pason/timelog/2/";
		String hdfsTimeLoc3 = "/data_lake/pason/timelog/3/";
		String hdfsTimeLoc4 = "/data_lake/pason/timelog/4/";
		String hdfsTimeLoc5 = "/data_lake/pason/timelog/5/";
		String hdfsTimeLoc6 = "/data_lake/pason/timelog/6/";
		String hdfsTimeLoc7 = "/data_lake/pason/timelog/7/";
		String hdfsTimeLoc8 = "/data_lake/pason/timelog/8/";
		String hdfsTimeLoc9 = "/data_lake/pason/timelog/9/";
		String hdfsTimeLoc10 = "/data_lake/pason/timelog/10/";
		String hdfsTimeLoc11 = "/data_lake/pason/timelog/11/";
		String hdfsTimeLoc12 = "/data_lake/pason/timelog/12/";
		String hdfsTimeLoc13 = "/data_lake/pason/timelog/13/";
		String hdfsTimeLoc14 = "/data_lake/pason/timelog/14/";
		String hdfsTimeLoc15 = "/data_lake/pason/timelog/15/";

		// String parquetFileLoc = null;

		File destinationFile = null;
		// File parquetFile = null;

		try {

			sourceFile = new File(srcFilePath);
			String parentFolder = sourceFile.getParent();
			fileName = sourceFile.getName();
			destinationFile = new File(parentFolder + "/..");
			parseFile = destinationFile.getCanonicalPath() + "/parsedfiles/" + fileName;

			// String parquetFileName = fileName.replace(".csv", ".parquet");

			// parquetFileLoc = destinationFile.getCanonicalPath() +"/parquetfiles/"+
			// parquetFileName;

			destinationFile = new File(parseFile);
			// parquetFile = new File(parquetFileLoc);

			// System.out.println(" parseDepthLogData parseFile "+parseFile);

			depthLogPW = new PrintWriter(parseFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		try {

			// File file = new File(srcFilePath);
			FileReader fileReader = new FileReader(sourceFile);
			br = new BufferedReader(fileReader);

			String wellname = fileName.substring(0, fileName.indexOf("_")).trim();

			StringBuffer headInfo = new StringBuffer();

			Map<String, Integer> mnemonicMap = new HashMap<String, Integer>();
			int index = 0;
			for (String mnemonic : FILE_HEADER) {
				headInfo.append(mnemonic).append(",");
				mnemonicMap.put(mnemonic, index);
				index++;
			}

			List<Integer> indexList = new ArrayList<Integer>();

			int lineNumber = 0;
			List<CVSColumnPostion> columnPostionList = null;
			columnPostionList = new ArrayList<CVSColumnPostion>();

			while ((line = br.readLine()) != null) {
				StringBuilder depthLogBuilder = new StringBuilder();
				// use comma as separator
				String[] depthLogInfo = line.split(cvsSplitBy);
				if (lineNumber == 0) {
					StringTokenizer stringTokenizer = new StringTokenizer(line, cvsSplitBy);
					CVSColumnPostion columnPostion = null;
					int counter = 0;
					while (stringTokenizer.hasMoreTokens()) {
						columnPostion = new CVSColumnPostion();
						String tokenValue = stringTokenizer.nextToken();
						if (tokenValue.contains("("))
							tokenValue = tokenValue.substring(0, tokenValue.indexOf("(")).trim();
						tokenValue = tokenValue.toUpperCase();
						String mnemonicMapping = mnemonicProperty.getProperty(tokenValue);

						Integer mnemonicIndex = 0;

						if (mnemonicMapping != null) {
							mnemonicIndex = mnemonicMap.get(mnemonicMapping.toUpperCase());
						} else {
							System.out.println(
									" <<<----- No Mnemomnic mapping found for CSV header value ---->>> " + tokenValue);
						}

						if (mnemonicIndex != null) {
							columnPostion.setDestinationIndex(mnemonicIndex);
							indexList.add(mnemonicIndex);
						}

						columnPostion.setSourceName(tokenValue);
						columnPostion.setDestinationName(mnemonicMapping);
						columnPostion.setSourceIndex(counter);

						counter++;
						columnPostionList.add(columnPostion);
					}

					Collections.sort(columnPostionList, new CVSColumnPostion());

					depthLogBuilder.append(headInfo);
					depthLogBuilder.append('\n');
					depthLogPW.write(depthLogBuilder.toString());

				}

				else {

					int arrLength = columnPostionList.get(columnPostionList.size() - 1).getDestinationIndex();
					String arr[] = new String[arrLength];
					if (depthLogInfo.length > 2)
						depthLogBuilder.append(wellname.trim() + ",");
					for (CVSColumnPostion columnPostion : columnPostionList) {
						int srcIdx = columnPostion.getSourceIndex();
						int destIdx = columnPostion.getDestinationIndex();
						String srcName = columnPostion.getSourceName();
						try {
							if (destIdx != 0 && srcIdx < depthLogInfo.length) {
								if ("YYYY/MM/DD".equalsIgnoreCase(srcName)) {
									arr[destIdx - 1] = depthLogInfo[srcIdx] + " " + depthLogInfo[srcIdx + 1];
								} else {
									arr[destIdx - 1] = depthLogInfo[srcIdx];
								}
							} else {
							}
						} catch (Exception exp) {
							exp.printStackTrace();
						}
					}

					for (String str : arr) {
						if (str != null && str != "") {
							depthLogBuilder.append(str).append(",");
						} else {
							depthLogBuilder.append(",");
						}
					}
					depthLogBuilder.append('\n');
					depthLogPW.write(depthLogBuilder.toString());
				}

				lineNumber++;
			}
			System.out.println("total line number processed from a file " + lineNumber);
			depthLogPW.close();

			// ConvertUtils.convertCsvToParquet(destinationFile, parquetFile);

			// copy CSV file to HDFS

			// copy CSV file to HDFS
			if (fileNumber <= 300) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc1);
			} else if (fileNumber > 300 && fileNumber <= 600) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc2);
			} else if (fileNumber > 900 && fileNumber <= 1200) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc3);
			}

			else if (fileNumber > 1200 && fileNumber <= 1500) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc4);
			}

			else if (fileNumber > 1500 && fileNumber <= 1800) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc5);
			}

			else if (fileNumber > 1800 && fileNumber <= 2100) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc6);
			}

			else if (fileNumber > 2100 && fileNumber <= 2400) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc7);
			} else if (fileNumber > 2400 && fileNumber <= 2700) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc8);
			}

			else if (fileNumber > 2700 && fileNumber <= 3000) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc9);
			} else if (fileNumber > 3000 && fileNumber <= 3300) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc10);
			} else if (fileNumber > 3300 && fileNumber <= 3600) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc11);
			}

			else if (fileNumber > 3600 && fileNumber <= 3900) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc12);
			}

			else if (fileNumber > 3900 && fileNumber <= 4200) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc13);
			}

			else if (fileNumber > 4200 && fileNumber <= 4500) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc14);
			}

			else if (fileNumber > 4500) {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc15);
			} else {
				HDFSUtility.copyFilesToHDFS(parseFile, hdfsTimeLoc);
			}

			System.out.println(" Processing done!");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
					sourceFile.delete();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// Reading Mnemonic mapping file from property file
	public static Properties readPropertyFile() {
		Properties mnemonicProp = new Properties();
		InputStream input = null;
		try {
			input = TimeLogParsing.class.getClassLoader().getResourceAsStream("mnemonic.properties");
			// load a properties file
			mnemonicProp.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return mnemonicProp;

	}

}
