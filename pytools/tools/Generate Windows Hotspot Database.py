#Usage:
#
# 1. Download all hotspot databases for Smart Client and put them into a directory, say ./101/
# 2. Modify windows_db_path and android_db_path
# 3. Run this script.
# 4. You must update id_name_pairs, hotspot_types or country_names if neccessary.
#
import sqlite3
import os
from sets import Set

id_name_pairs = {5577:'CMCC',
                 5370:'Meteor',
                 5538:'Orange France',
                 5586:'M3',
                 5728:'CMHK',
                 5693:'Linkem',
                 3994:'Wi2',
                 3993:'KT',
                 5670:'WIFLY',
                 5618:'BTOpenzone',
                 3908:'Trustive Limited'}

hotspot_types = {1 : 'Airport',
                 2 : 'Cafe',
                 3 : 'Convention Center',
                 4 : 'Office',
                 5 : 'Petrol Station',
                 6 : 'Hotel',
                 7 : 'Rail',
                 8 : 'Restaurant',
                 9 : 'Stores Shops',
                 10 : 'Others',
                 #Shouldn't have 13, the error exist in GB-All
                 13 : 'Others'}

country_names = {'BL': 'Saint BarthELemy',
                 'CN': 'China',
                 'DE': 'Germany',
                 'ES': 'Spain',
                 'FR': 'France',
                 'GB': 'United Kingdom',
                 'HK': 'Hong Kong',
                 'IE': 'Ireland',
                 'IT': 'Italy',
                 'JM': 'Jamaica',
                 'JP': 'Japan',
                 'KR': 'Korea Republic',
                 'MX': 'Mexico',
                 'NZ': 'New Zealand',
                 'PT': 'Portugal',
                 'TW': 'Taiwan',
                 'US': 'United States'
                 }


def create_table_if_not_exist(windows_db_path):
    windows_db_conn = sqlite3.connect(windows_db_path)
    windows_cur = windows_db_conn.cursor()
    windows_cur.execute('CREATE TABLE IF NOT EXISTS hotspot ( '
                    '    id                 INTEGER,'
                    '    hotspot_id         TEXT,'
                    '    ssid               TEXT,'
                    '    customer_name      TEXT,'
                    '    location_id        TEXT,'
                    '    country            TEXT,'
                    '    province           TEXT,'
                    '    city               TEXT,'
                    '    county             TEXT,'
                    '    max_speed          INTEGER,'
                    '    CHAP               INTEGER,'
                    '    static_dns         TEXT,'
                    '    dns_ip             TEXT,'
                    '    site_name          TEXT,'
                    '    site_name_localize TEXT,'
                    '    site_type          TEXT,'
                    '    site_subtype       TEXT,'
                    '    site_address       TEXT,'
                    '    site_address_local TEXT,'
                    '    site_phone         TEXT,'
                    '    client_supported   TEXT,'
                    '    longitute          TEXT,'
                    '    latitude           TEXT,'
                    '    time_zone          TEXT,'
                    '    coverage_area      TEXT,'
                    '    open_time          TEXT '
                    ');')
    windows_db_conn.commit()

def get_customer_name(customer_id):
    return id_name_pairs[customer_id]

def export_hotspotinfo(windows_db_path, android_db_path):
    print "process: ", android_db_path
    
    windows_db_conn = sqlite3.connect(windows_db_path)
    android_db_conn = sqlite3.connect(android_db_path)
    
    windows_cur = windows_db_conn.cursor()
    android_cur = android_db_conn.cursor()
    
    android_cur.execute("select latitude, longitude, HotspotName, HotspotSSID, HotspotType, Address, City, Country, LocationID, OpId from hotspots")
    for row in android_cur:
        row_tmp = list(row)
        
        #Translate hotspot type to its string presentation.
        row_tmp[4] = hotspot_types[row[4]]
        
        #Translate country name
        row_tmp[7] = country_names[row[7]]
        
        #Translate OpId to customer name
        row_tmp[9] = get_customer_name(row[9])
        
        windows_cur.execute("insert into hotspot (latitude, longitute, site_name, SSID, site_type, site_address, city, country, location_id, customer_name) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row_tmp)
    windows_db_conn.commit()

def get_ssid_list(android_db_path):
    #print "process: ", android_db_path
    android_db_conn = sqlite3.connect(android_db_path)
    android_cur = android_db_conn.cursor()
    android_cur.execute("select HotspotSSID from hotspots group by HotspotSSID")
    ssid_list = []
    for ssid in android_cur:
        ssid_list.append(ssid);
    return ssid_list

if __name__ == "__main__":
    windows_db_path = "D:\\doc\\Aicent Project\\ACM\\01_Customer_CMHK\\Hotspot database for windows\\pb.dat"
    android_db_path = "D:\\doc\\Aicent Project\\ACM\\01_Customer_CMHK\\Hotspot database for windows\\cmhk105\\cmhk\\"
    create_table_if_not_exist(windows_db_path)
    sqlite_files = filter(lambda x: x.endswith('dat'), os.listdir(android_db_path))
    ssid_list = []
    for db in sqlite_files:
        export_hotspotinfo(windows_db_path, android_db_path + db)
        ssid_list = ssid_list + get_ssid_list(android_db_path + db)
    print Set(ssid_list)
        
    
