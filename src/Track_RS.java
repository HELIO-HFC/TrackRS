// tracking of the radiosources of the HFC
// optional parameters: start date, end date, zone_x, zone_y, output_dir

import java.sql.*;
import java.util.*;
/*import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;*/
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.*;
/*import java.util.Calendar;
import java.util.ArrayList;*/

public class Track_RS {

	static ConnDb c;
	static SimpleDateFormat full_date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static int zone_x, zone_y;

	public static void main(String[] args) {
		String tbsp = "hfc1test"; 					// name of the tablespace or the schema
		//String host = "helio-fc1.obspm.fr"; 	// name of the database host
		String host = "localhost"; 	// name of the database host
		String output_dir = "."; // Output directory for tracking csv files (X.Bonnin, 21-JAN-2014)
		Date start_date=null, end_date=null;
		Map<String, ArrayList<String>> tab_track = new HashMap<String, ArrayList<String>>();
		SimpleDateFormat date_format = new SimpleDateFormat("yyyy-MM-dd");
		//date_format.setTimeZone(TimeZone.getTimeZone("UTC"));
		SimpleDateFormat full_date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//full_date_format.setTimeZone(TimeZone.getTimeZone("UTC"));

		// Check arguments
		if (args.length == 2) {
			// initializing host and tbsp
			tbsp = args[0];
			host = args[1];
			// setting default values for parameters
			// getting min and max DATE_OBS from the database
			String sql_query = "SELECT MIN(DATE_OBS) AS MIN, MAX(DATE_OBS) AS MAX FROM VIEW_RS_GUI";
			try {
				Statement s = c.conn.createStatement(
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
				//System.out.println(sql_query);
				ResultSet rs = s.executeQuery(sql_query);
				rs.first();
				start_date = rs.getDate(1);
				end_date = rs.getDate(2);
			} catch (SQLException sqle) {
				System.err.println("Error getting min and max DATE_OBS");
				System.err.println(sqle.getSQLState() + " : " + sqle);
				while ((sqle = sqle.getNextException()) != null) {
					System.err.println(sqle.getSQLState() + " : " + sqle);
				}
			}
			zone_x = 4;
			zone_y = 4;
		} else if (args.length == 6) {
			// initializing host and tbsp
				tbsp = args[0];
				host = args[1];
			// getting arguments
			try {
				start_date = date_format.parse(args[2]);
				end_date = date_format.parse(args[3]);
				//System.out.println(args[0] + " or " + args[1]);
			} catch (ParseException parse_e) {
				System.out.println("Error parsing date from " + args[2] + " or " + args[3]);
			}
			zone_x = Integer.parseInt(args[4]);
			zone_y = Integer.parseInt(args[5]);
		} else if (args.length == 7) {

			// initializing host and tbsp
				tbsp = args[0];
				host = args[1];
			// getting arguments
			try {
				start_date = date_format.parse(args[2]);
				end_date = date_format.parse(args[3]);
				//System.out.println(args[0] + " or " + args[1]);
			} catch (ParseException parse_e) {
				System.out.println("Error parsing date from " + args[2] + " or " + args[3]);
			}
			zone_x = Integer.parseInt(args[4]);
			zone_y = Integer.parseInt(args[5]);
			output_dir = args[6];
		} else {
			System.out.println("Usage: Track_RS tbsp host [startdate enddate zonex zoney output_dir]");
			System.exit(0);
		}

		// Open a connection to the database
		c = new ConnDb();
		c.init(tbsp, host);

		// System.out.println("start_date=" + start_date.toString() + " end_date=" + end_date.toString() + " zone_x=" + zone_x + " zone_y=" + zone_y);

		Calendar cal = GregorianCalendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		cal.setTime(start_date);
		Calendar cal_end = GregorianCalendar.getInstance();
		cal_end.setTimeZone(TimeZone.getTimeZone("UTC"));
		cal_end.setTime(end_date);
		cal_end.add(Calendar.DATE, 1);
		System.out.println("start_date=" + date_format.format(cal.getTime()) + " end_date=" +  date_format.format(cal_end.getTime()) + " zone_x=" + zone_x + " zone_y=" + zone_y);

		// initializing tab_track for start and end dates from table VIEW_RS_GUI
		cal.add(Calendar.DATE, -2);
		cal_end.add(Calendar.DATE, 2);
		String sql_query = "SELECT TRACK_ID, ID_RS, DATE_OBS, FEAT_X_PIX, FEAT_Y_PIX FROM VIEW_RS_GUI WHERE TRACK_ID!='NULL' AND DATE(DATE_OBS) BETWEEN '" + date_format.format(cal.getTime()) + "' AND '" + date_format.format(cal_end.getTime()) + "' ORDER BY DATE_OBS ASC";
		try {
			Statement st = c.conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			//System.out.println(sql_query);
			ResultSet rst = st.executeQuery(sql_query);
			ArrayList <String> al_track_id = new ArrayList<String>();
			ArrayList <String> al_id_rs = new ArrayList<String>();
			ArrayList <String>  al_date_obs = new ArrayList<String>();
			ArrayList <String>  al_feat_x = new ArrayList<String>();
			ArrayList <String>  al_feat_y = new ArrayList<String>();
			if (rst.first()) {
				while(rst.next()) {
					al_track_id.add(rst.getString("TRACK_ID"));
					al_id_rs.add(rst.getString("ID_RS"));
					al_date_obs.add(rst.getString("DATE_OBS"));
					al_feat_x.add(rst.getString("FEAT_X_PIX"));
					al_feat_y.add(rst.getString("FEAT_Y_PIX"));
				}
			}
			tab_track.put("TRACK_ID", al_track_id);
			tab_track.put("ID_RS", al_id_rs);
			tab_track.put("DATE_OBS",al_date_obs);
			tab_track.put("FEAT_X_PIX",al_feat_x);
			tab_track.put("FEAT_Y_PIX",al_feat_y);
		} catch (SQLException sqle) {
			System.err.println("Error insert " + sql_query);
			System.err.println(sqle.getSQLState() + " : " + sqle);
			while ((sqle = sqle.getNextException()) != null) {
				System.err.println(sqle.getSQLState() + " : " + sqle);
			}
		}
		//print_tab_track(tab_track);
		// comme back to initial date range
		cal.add(Calendar.DATE, 2);
		cal_end.add(Calendar.DATE, -2);
		while(!date_format.format(cal.getTime()).equals(date_format.format(cal_end.getTime()))) {
		//while(!cal.equals(cal_end)) {
			sql_query = "SELECT * FROM VIEW_RS_GUI WHERE DATE(DATE_OBS)='" + date_format.format(cal.getTime()) + "' ORDER BY DATE_OBS ASC";
			try {
				Statement s = c.conn.createStatement(
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_READ_ONLY);
				//System.out.println(sql_query);
				ResultSet rs = s.executeQuery(sql_query);
				// make a first sort among sources of the day
				Map<String, ArrayList<String>> tmp_track = new HashMap<String, ArrayList<String>>();
				//int tmp_id_track = 0;
				while(rs.next()) {
					//System.out.println(rs.getInt("ID_RS") + " " + rs.getDate("DATE_OBS").toString() + " " + rs.getInt("FEAT_X_PIX") + " " + rs.getInt("FEAT_Y_PIX"));
					// check if the source is already tracked
					if (tab_track.get("ID_RS").contains(rs.getString("ID_RS"))) {
						//System.out.println("ID_RS: " + rs.getInt("ID_RS") + " is already tracked");
						continue;
					}
					if (tmp_track.isEmpty()) {
						System.out.println("Initializing temp tab_track with new track_id=" + rs.getString("ID_RS"));
						ArrayList <String> al_track_id = new ArrayList<String>();
						al_track_id.add(rs.getString("ID_RS"));
						tmp_track.put("TRACK_ID", al_track_id);
						ArrayList <String> al_id_rs = new ArrayList<String>();
						al_id_rs.add(rs.getString("ID_RS"));
						tmp_track.put("ID_RS", al_id_rs);
						ArrayList <String>  al_date_obs = new ArrayList<String>();
						al_date_obs.add(rs.getString("DATE_OBS"));
						tmp_track.put("DATE_OBS",al_date_obs);
						ArrayList <String>  al_feat_x = new ArrayList<String>();
						al_feat_x.add(rs.getString("FEAT_X_PIX"));
						tmp_track.put("FEAT_X_PIX",al_feat_x);
						ArrayList <String>  al_feat_y = new ArrayList<String>();
						al_feat_y.add(rs.getString("FEAT_Y_PIX"));
						tmp_track.put("FEAT_Y_PIX",al_feat_y);
					} else {
						int index = -1;
						//print_tab_track(tmp_track);
						int nb_track_id = tmp_track.get("TRACK_ID").size();
						for (int i=0; i<nb_track_id; i++) {
							//System.out.println("Comparing " + rs.getString("ID_RS") + " at " +  rs.getString("DATE_OBS") + " " + rs.getString("FEAT_X_PIX") + " " + rs.getString("FEAT_Y_PIX") +" to track_id " + tab_track.get("TRACK_ID").get(i) + " " + tab_track.get("ID_RS").get(i) + " " + tab_track.get("DATE_OBS").get(i) + " " + tab_track.get("FEAT_X_PIX").get(i)+ " " + tab_track.get("FEAT_Y_PIX").get(i));
							boolean match = compare_source(rs.getString("DATE_OBS"), rs.getInt("FEAT_X_PIX"), rs.getInt("FEAT_Y_PIX"), tmp_track, i);
							// check if its really match with the last source of this track_id
							if (match) {
								//System.out.println("source id " + rs.getString("ID_RS") + " at " +  rs.getString("DATE_OBS") + " " + rs.getString("FEAT_X_PIX") + " " + rs.getString("FEAT_Y_PIX") +" match with track_id " + tmp_track.get("TRACK_ID").get(i) + " " + tmp_track.get("ID_RS").get(i) + " " + tmp_track.get("DATE_OBS").get(i) + " " + tmp_track.get("FEAT_X_PIX").get(i)+ " " + tmp_track.get("FEAT_Y_PIX").get(i) + " checking more ...");
								// get the last source for this track_id
								int last_index = i;
								for (int j=0; j<nb_track_id; j++) {
									if (tmp_track.get("TRACK_ID").get(i) == tmp_track.get("TRACK_ID").get(j))
										last_index = j;
								}
								if (last_index != i) {
									boolean match_last = compare_source(rs.getString("DATE_OBS"), rs.getInt("FEAT_X_PIX"), rs.getInt("FEAT_Y_PIX"), tmp_track, last_index);
									if (match_last) index = last_index;
									//else index = i;
									else continue; //go to the next if last rs of this track does not match
								}
								else index = i;
							}
							else continue;

							break;
						}
						ArrayList <String> vec = tmp_track.get("TRACK_ID");
						if ((index != -1) && !check_same_date(rs.getString("DATE_OBS"), tmp_track, tmp_track.get("TRACK_ID").get(index))) {
								// add this source with the existent track_id
								//System.out.println("source id " + rs.getString("ID_RS") + " at " +  rs.getString("DATE_OBS") + " belongs to track_id " + tab_track.get("TRACK_ID").get(index));
								System.out.println("source id " + rs.getString("ID_RS") + " at " +  rs.getString("DATE_OBS") + " " + rs.getString("FEAT_X_PIX") + " " + rs.getString("FEAT_Y_PIX") +" match with track_id " + tmp_track.get("TRACK_ID").get(index) + " " + tmp_track.get("ID_RS").get(index) + " " + tmp_track.get("DATE_OBS").get(index) + " " + tmp_track.get("FEAT_X_PIX").get(index)+ " " + tmp_track.get("FEAT_Y_PIX").get(index));
								vec.add(tmp_track.get("TRACK_ID").get(index));
						} else {
							// insert the source in tab_track as a new track_id
							System.out.println("New track_id=" + rs.getString("ID_RS"));
							vec.add(rs.getString("ID_RS"));

						}
						tmp_track.put("TRACK_ID",vec);
						vec = tmp_track.get("ID_RS");
						vec.add(rs.getString("ID_RS"));
						tmp_track.put("ID_RS",vec);
						vec = tmp_track.get("DATE_OBS");
						vec.add(rs.getString("DATE_OBS"));
						tmp_track.put("DATE_OBS",vec);
						vec = tmp_track.get("FEAT_X_PIX");
						vec.add(rs.getString("FEAT_X_PIX"));
						tmp_track.put("FEAT_X_PIX",vec);
						vec = tmp_track.get("FEAT_Y_PIX");
						vec.add(rs.getString("FEAT_Y_PIX"));
						tmp_track.put("FEAT_Y_PIX",vec);
					}
				} // end of loop on rs for this date
				//print_tab_track(tmp_track);
				//System.out.println("tab track size=" + tab_track.get("TRACK_ID").size());

				if (!tmp_track.isEmpty()) { // go to the next date
				// Now for each temporary track_id checks if it can be linked to existing track_id (+ or - 1 day)

				// make a tab with unique temporary track_id number
				int nb_tmp_track_id = tmp_track.get("TRACK_ID").size();
				ArrayList <String>  t_track_ids = new ArrayList<String>();
				for (int i=0; i<nb_tmp_track_id; i++) {
					if (!t_track_ids.contains(tmp_track.get("TRACK_ID").get(i))) {
							t_track_ids.add(tmp_track.get("TRACK_ID").get(i));
					}
				}
				System.out.println("Linking with existing track_id...");
				//Initialize tab_track if it's empty
				if (tab_track.get("TRACK_ID").size() == 0) {
					System.out.println("Initializing tab_track");
					ArrayList <String> al_track_id = new ArrayList<String>();
					ArrayList <String> al_id_rs = new ArrayList<String>();
					ArrayList <String>  al_date_obs = new ArrayList<String>();
					ArrayList <String>  al_feat_x = new ArrayList<String>();
					ArrayList <String>  al_feat_y = new ArrayList<String>();
					for (int i=0; i<nb_tmp_track_id; i++) {
						System.out.println("adding " + tmp_track.get("TRACK_ID").get(i) + " " + tmp_track.get("ID_RS").get(i));
						al_track_id.add(tmp_track.get("TRACK_ID").get(i));
						al_id_rs.add(tmp_track.get("ID_RS").get(i));
						al_date_obs.add(tmp_track.get("DATE_OBS").get(i));
						al_feat_x.add(tmp_track.get("FEAT_X_PIX").get(i));
						al_feat_y.add(tmp_track.get("FEAT_Y_PIX").get(i));
					}
						tab_track.put("TRACK_ID", al_track_id);
						tab_track.put("ID_RS", al_id_rs);
						tab_track.put("DATE_OBS",al_date_obs);
						tab_track.put("FEAT_X_PIX",al_feat_x);
						tab_track.put("FEAT_Y_PIX",al_feat_y);
				} else {
					for (int curr_trk_id=0; curr_trk_id<t_track_ids.size(); curr_trk_id++) {
						//System.out.println("Processing temporary track_id: " + curr_trk_id);
						// get ids with track_id equals to curr_trk_id
						ArrayList <Integer>  t_ids = new ArrayList<Integer>();
						for (int i=0; i<nb_tmp_track_id; i++) {
							if (tmp_track.get("TRACK_ID").get(i).equals(t_track_ids.get(curr_trk_id))) {
									t_ids.add(i);
							}
						}
						int min_id = t_ids.get(0);
						//System.out.println("TRACK_ID: " + t_track_ids.get(curr_trk_id) + " min: " + min_id + " nb sources:" + t_ids.size());
					//System.out.println(rs.getInt("ID_RS") + " " + rs.getDate("DATE_OBS").toString() + " " + rs.getInt("FEAT_X_PIX") + " " + rs.getInt("FEAT_Y_PIX"));

					// check if there is a source in tab_track with the criteria
						int index = -1;
						int nb_track_id = tab_track.get("TRACK_ID").size();
						for (int i=0; i<nb_track_id; i++) {
							//System.out.println("Comparing " + rs.getString("ID_RS") + " at " +  rs.getString("DATE_OBS") + " " + rs.getString("FEAT_X_PIX") + " " + rs.getString("FEAT_Y_PIX") +" to track_id " + tab_track.get("TRACK_ID").get(i) + " " + tab_track.get("ID_RS").get(i) + " " + tab_track.get("DATE_OBS").get(i) + " " + tab_track.get("FEAT_X_PIX").get(i)+ " " + tab_track.get("FEAT_Y_PIX").get(i));
							// go to next if track_id already exists in tmp_track, means that a source at this date have already been linked with a TRACK_ID of tab_track
							if (tmp_track.get("TRACK_ID").contains(tab_track.get("TRACK_ID").get(i))) continue;
							boolean match = compare_source(tmp_track.get("DATE_OBS").get(min_id), Integer.valueOf(tmp_track.get("FEAT_X_PIX").get(min_id)), Integer.valueOf(tmp_track.get("FEAT_Y_PIX").get(min_id)), tab_track, i);
							// check if its really match with the last source of this track_id
							if (match) {
								//System.out.println(tmp_track.get("TRACK_ID").get(min_id) + " source id " + tmp_track.get("ID_RS").get(min_id) + " at " +  tmp_track.get("DATE_OBS").get(min_id) + " " + tmp_track.get("FEAT_X_PIX").get(min_id) + " " + tmp_track.get("FEAT_Y_PIX").get(min_id) +" match with track_id " + tab_track.get("TRACK_ID").get(i) + " " + tab_track.get("ID_RS").get(i) + " " + tab_track.get("DATE_OBS").get(i) + " " + tab_track.get("FEAT_X_PIX").get(i)+ " " + tab_track.get("FEAT_Y_PIX").get(i) + " checking more ...");
								// get the last source for this track_id
								int last_index = i;
								for (int j=0; j<nb_track_id; j++) {
									if (tab_track.get("TRACK_ID").get(i) == tab_track.get("TRACK_ID").get(j))
										last_index = j;
								}
								if (last_index != i) {
									boolean match_last = compare_source(tmp_track.get("DATE_OBS").get(min_id), Integer.valueOf(tmp_track.get("FEAT_X_PIX").get(min_id)), Integer.valueOf(tmp_track.get("FEAT_Y_PIX").get(min_id)), tab_track, last_index);
									if (match_last) index = last_index;
									//else index = i;
									else continue; //go to the next if last rs of this track does not match
								}
								else index = i;
							}
							else continue;
							break;
						}
						if (index != -1) {
							// add this source with the existent track_id
							//System.out.println("source id " + rs.getString("ID_RS") + " at " +  rs.getString("DATE_OBS") + " belongs to track_id " + tab_track.get("TRACK_ID").get(index));
							System.out.println(tmp_track.get("TRACK_ID").get(min_id) + " source id " + tmp_track.get("ID_RS").get(min_id) + " at " +  tmp_track.get("DATE_OBS").get(min_id) + " " + tmp_track.get("FEAT_X_PIX").get(min_id) + " " + tmp_track.get("FEAT_Y_PIX").get(min_id) + " match with track_id " + tab_track.get("TRACK_ID").get(index) + " " + tab_track.get("ID_RS").get(index) + " " + tab_track.get("DATE_OBS").get(index) + " " + tab_track.get("FEAT_X_PIX").get(index)+ " " + tab_track.get("FEAT_Y_PIX").get(index));
							// change all the RS with the same temp TRACK_ID to the corresponding TRACK_ID
							for (int j=0; j<t_ids.size(); j++) {
								System.out.println("Changing " + tab_track.get("TRACK_ID").get(index) + " " + tmp_track.get("ID_RS").get(t_ids.get(j)));
								tmp_track.get("TRACK_ID").set(t_ids.get(j), tab_track.get("TRACK_ID").get(index));
							}
						} else {
							// insert the source in tab_track as a new track_id
							//System.out.println("New track_id=" + rs.getString("ID_RS"));
							//for (int i=min_id; i<=max_id; i++) {
							for (int j=0; j<t_ids.size(); j++) {
								System.out.println("adding " + tmp_track.get("ID_RS").get(min_id) + " " + tmp_track.get("ID_RS").get(t_ids.get(j)));
								tmp_track.get("TRACK_ID").set(t_ids.get(j), tmp_track.get("ID_RS").get(min_id));
							}
								//vec.add(rs.getString("ID_RS"));
						}
						//for (int i=min_id; i<=max_id; i++) {
						for (int i=0; i<t_ids.size(); i++) {
							ArrayList <String> vec = tab_track.get("TRACK_ID");
							vec.add(tmp_track.get("TRACK_ID").get(t_ids.get(i)));
							tab_track.put("TRACK_ID",vec);
							vec = tab_track.get("ID_RS");
							vec.add(tmp_track.get("ID_RS").get(t_ids.get(i)));
							tab_track.put("ID_RS",vec);
							vec = tab_track.get("DATE_OBS");
							vec.add(tmp_track.get("DATE_OBS").get(t_ids.get(i)));
							tab_track.put("DATE_OBS",vec);
							vec = tab_track.get("FEAT_X_PIX");
							vec.add(tmp_track.get("FEAT_X_PIX").get(t_ids.get(i)));
							tab_track.put("FEAT_X_PIX",vec);
							vec = tab_track.get("FEAT_Y_PIX");
							vec.add(tmp_track.get("FEAT_Y_PIX").get(t_ids.get(i)));
							tab_track.put("FEAT_Y_PIX",vec);
						}
						//print_tab_track(tab_track);
					}
					System.out.println("Tracking sources for "+ date_format.format(cal.getTime()) + " completed!");
				}
			}
			} catch (SQLException sqle) {
				System.err.println("Error getting radio sources for " + date_format.format(cal.getTime()));
				System.err.println(sqle.getSQLState() + " : " + sqle);
				while ((sqle = sqle.getNextException()) != null) {
					System.err.println(sqle.getSQLState() + " : " + sqle);
				}
			}

			//print_tab_track(tab_track);
			//System.exit(0);
			// make a CSV file for this date
			if (!tab_track.isEmpty())
				make_csv_file(cal.getTime(), tab_track, output_dir);

			cal.add(Calendar.DATE, 1);
		}
		//print_tab_track(tab_track);
		//System.exit(0);
		// insert in table TRACK_RS
		//int nb_track_id = tab_track.get("TRACK_ID").size();
		// for (int j=0; j<nb_track_id; j++) {
		// 	if (!alreadyTracked(tab_track.get("ID_RS").get(j))) {
		// 		String sql_insert = "INSERT INTO RS_TRACKING (TRACK_ID, RS_ID,RUN_DATE) VALUES ( ";
		// 		sql_insert = sql_insert + tab_track.get("TRACK_ID").get(j) + "," ;
		// 		sql_insert = sql_insert + tab_track.get("ID_RS").get(j) + "," ;
		// 		Calendar cal_now = Calendar.getInstance();
		// 		sql_insert = sql_insert +  "'" + full_date_format.format(cal_now.getTime()) + "')";
		// 		//System.out.println(sql_insert);
		// 		try {
		// 			Statement s = c.conn.createStatement();
		// 			s.executeUpdate(sql_insert);
		// 			s.close();
		// 		} catch (SQLException sqle) {
		// 			System.err.println("Eerror insert " + sql_insert);
		// 			System.err.println(sqle.getSQLState() + " : " + sqle);
		// 			while ((sqle = sqle.getNextException()) != null) {
		// 				System.err.println(sqle.getSQLState() + " : " + sqle);
		// 			}
		// 		}
		// 	}
		// }
		c.commit();
		// get unique track_id values
		//print_tab_track(tab_track);
		c.ferme();
	}

	private static boolean alreadyTracked(String rs_id) {
		String query = "SELECT RS_ID FROM RS_TRACKING WHERE RS_ID=" + rs_id;
		try  {
			Statement s = c.conn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_READ_ONLY);
			//System.out.println(query);
			ResultSet rs = s.executeQuery(query);
			if (rs.next()) return true;
			else return false;
		}  catch (SQLException sqle) {
			System.err.println("Error SQL");
			System.err.println(sqle.getSQLState() + " : " + sqle);
			while ((sqle = sqle.getNextException()) != null) {
				System.err.println(sqle.getSQLState() + " : " + sqle);
			}
			return false;
		}
	}

	// return false if
	// date_obs are not the same year
	// date_obs have the same time, date_obs is not tomorrow
	// FEAT_X(Y)_PIX is out of the zone
	private static boolean compare_source(String date_obs, int feat_x_pix, int feat_y_pix,
										Map<String, ArrayList<String>> tab_track, int id_tab_track) {

		try {
			Date track_date = full_date_format.parse(tab_track.get("DATE_OBS").get(id_tab_track));
			Calendar cal_track_date = GregorianCalendar.getInstance();
			cal_track_date.setTime(track_date);
			int track_x_pix = Integer.parseInt(tab_track.get("FEAT_X_PIX").get(id_tab_track));
			int track_y_pix = Integer.parseInt(tab_track.get("FEAT_Y_PIX").get(id_tab_track));

			Date feat_date = full_date_format.parse(date_obs);
			Calendar cal_feat_date = GregorianCalendar.getInstance();
			cal_feat_date.setTimeZone(TimeZone.getTimeZone("UTC"));
			cal_feat_date.setTime(feat_date);

			if (cal_feat_date.get(Calendar.YEAR) != cal_track_date.get(Calendar.YEAR)) return false;
			//System.out.println("Same year");
			// test if the source date is the next day or the same day
			int date_int = cal_feat_date.get(Calendar.DAY_OF_YEAR) - cal_track_date.get(Calendar.DAY_OF_YEAR);
			if ((date_int != 1) && (date_int != 0)) return false;
			//System.out.println("Not same day or tomorrow");
			if (feat_date.compareTo(track_date) == 0) return false;
			//System.out.println("Time are different");
			//System.out.println(rs.getString("DATE_OBS") + " "+cal_cur_date_obs.get(Calendar.DAY_OF_YEAR)+" "+date_obs.toString()+ " day of year="+cal_date_obs.get(Calendar.DAY_OF_YEAR)+" tst_dayinterval="+tst_dayinterval);
			// if next day, zone_x is enlarged
			int zone_x_c = 0, zone_y_c = 0;
			if (date_int == 1) {
				zone_x_c = zone_x + 6;
				zone_y_c = zone_y + 2;
			}
			else {
				zone_x_c = zone_x;
				zone_y_c = zone_y;
			}
			if (Math.abs(feat_x_pix - track_x_pix) > zone_x_c) return false;
			if (Math.abs(feat_y_pix - track_y_pix) > zone_y_c) return false;

			return true;
		} catch (ParseException pe) {
			System.out.println("compare_source error parsing date");
			return false;
		}
	}

	// check if for a given id_tab_track there is a rs with the date date_obs
	private static boolean check_same_date(String date_obs, Map<String, ArrayList<String>> tab_track, String id_tab_track) {

		try {
			Date feat_date = full_date_format.parse(date_obs);
			for (int i=0; i<tab_track.get("TRACK_ID").size(); i++) {
				if (tab_track.get("TRACK_ID").get(i).equals(id_tab_track)) {
					Date track_date = full_date_format.parse(tab_track.get("DATE_OBS").get(i));
					if (feat_date.compareTo(track_date) == 0) {
						//System.out.println("date_obs=" + date_obs +  " track date=" + tab_track.get("DATE_OBS").get(i));
						return true;
					}
				}
			}
			return false;
		} catch (ParseException pe) {
			System.out.println("check_same_date error parsing date");
			return false;
		}
	}

	private static void make_csv_file(Date date, Map<String, ArrayList<String>> tab_track, String output_dir) {

		ArrayList <String> lines = new ArrayList<String>();

		int nb_track_id = tab_track.get("TRACK_ID").size();
		//System.out.println("nb_track_id=" + nb_track_id);
		SimpleDateFormat dt_fmt_simple = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dt_fmt_full = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		SimpleDateFormat dt_fmt_sql = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd'T'HHmmss");
		String institut = "OBSPM", code = "TRACKRS", version = "100", feature = "RADIOSOURCES";
		String enc_met = "", person = "Christian Renié", contact = "Christian.Renie@obspm.fr", reference = "";
		String csv_filename = "trackrs_" + version + "_" + date_format.format(date) + "_nan_track.csv";
		csv_filename = output_dir + File.separator + csv_filename;
		try {
		for (int j=0; j<nb_track_id; j++) {
			Date date_obs = dt_fmt_simple.parse(tab_track.get("DATE_OBS").get(j));
			if(dt_fmt_simple.format(date_obs).equals(dt_fmt_simple.format(date))) {
				// check if this RS_ID has already been tracked
				if (!alreadyTracked(tab_track.get("ID_RS").get(j))) {
					Date dt = dt_fmt_sql.parse(tab_track.get("DATE_OBS").get(j));
					String line = "1;"+tab_track.get("TRACK_ID").get(j)+";"+tab_track.get("ID_RS").get(j);
					line = line+";"+dt_fmt_full.format(dt)+";"+tab_track.get("FEAT_X_PIX").get(j)+";"+tab_track.get("FEAT_Y_PIX").get(j);
					line = line +";" + dt_fmt_full.format(date);
					lines.add(line);
				}
			}
		}
		int nb_lines = lines.size();
		if (nb_lines != 0) {
			File csv_file = new File(csv_filename);
			if (csv_file.exists()) csv_file.delete();
			csv_file.createNewFile();
			BufferedWriter fichin = new BufferedWriter(new FileWriter(csv_filename, true));
			String line =  "FRC_INFO_ID;TRACK_ID;RS_ID;DATE_OBS;FEAT_X_PIX;FEAT_Y_PIX;RUN_DATE";
			fichin.write(line, 0,line.length());
			fichin.newLine();
			fichin.flush();
			for (int i=0; i<nb_lines; i++) {
				fichin.write(lines.get(i), 0,lines.get(i).length());
				fichin.newLine();
				fichin.flush();
			}
			fichin.close();
		}
		} catch(IOException e) {
			System.out.println("make_csv_file: I/O error: " + csv_filename);
		} catch (ParseException pe) {
			System.out.println("make_csv_file: error parsing date");
		}

	}

	private static void print_tab_track(Map<String, ArrayList<String>> tab_track) {
		int nb_track_id = tab_track.get("TRACK_ID").size();
		ArrayList <String>  t_track_ids = new ArrayList<String>();
		for (int i=0; i<nb_track_id; i++) {
			if (!t_track_ids.contains(tab_track.get("TRACK_ID").get(i)) )
					t_track_ids.add(tab_track.get("TRACK_ID").get(i));
		}
		System.out.println("Size of tab_track=" + t_track_ids.size());
		for (int i=0; i<t_track_ids.size(); i++) {
			int curr_track_id = Integer.parseInt(t_track_ids.get(i));
			System.out.println("TRACK_ID " +  curr_track_id);
			for (int j=0; j<nb_track_id; j++) {
				/*System.out.println(tab_track.get("ID_RS").get(j) + " " +
						tab_track.get("DATE_OBS").get(j) + " " +
						tab_track.get("FEAT_X_PIX").get(j) + " " +
						tab_track.get("FEAT_Y_PIX").get(j) ); */
				int track_id = Integer.parseInt(tab_track.get("TRACK_ID").get(j));
				if (track_id == curr_track_id)
				//if (tab_track.get("TRACK_ID").get(j) == t_track_ids.get(i))
					System.out.println(tab_track.get("ID_RS").get(j) + " " +
										tab_track.get("DATE_OBS").get(j) + " " +
										tab_track.get("FEAT_X_PIX").get(j) + " " +
										tab_track.get("FEAT_Y_PIX").get(j) );
			}
		}
	}

}