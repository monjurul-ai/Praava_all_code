import psycopg2
import cx_Oracle 
import pandas as pd
from datetime import timedelta, datetime

def db():
		ip = '20.207.91.194'
		port = 15022
		SID = 'PRHLIVE'
		dsn_tns = cx_Oracle.makedsn(ip, port, SID)
		ict_conn = cx_Oracle.connect('prhlivero', 'prhlivero421', dsn_tns)
		ict_cursor = ict_conn.cursor()

		conn1 = psycopg2.connect(database="dev_lab_tat_com", user="consult", password="consult1234", host="20.198.153.150",
						port="5432")
		cur1 = conn1.cursor()
		return ict_conn,ict_cursor,conn1,cur1


def labdata1(ict_conn, ict_cursor):
		df_his = pd.read_sql('''

	   SELECT
		DISTINCT t.lab_order_id,
		p.mrno,
		p.patientname,
		p.age,
		decode(p.ageunit, 1, 'Year', 2, 'Month', 3, 'Day') age_unit,
		decode(p.genderid, 1, 'Male', 2, 'Female') gender,
		p.mobileno,
		p.email, 
											  l.lab_service_name,
		t.lab_service_id,
		so.profile_id,
		s.SERVICE_ID ,
		c.service_center_name, 
											  t.ordered_date,
		t.samplecollected_date,
		t.samplegenerated_date,
		to_char(t.sampleaccepted_date, 'YYYY-MM-DD hh24:mi') as a_date_str,
		r.certified_date,
		r.updateddatetime,

							 (
		SELECT
			DISTINCT o.createddatetime AS printing_time
		FROM
			PRHLIVE.print_audit_details o
		WHERE
			o.createddatetime =(
			SELECT
				min(x.createddatetime)
			FROM
				PRHLIVE.print_audit_details x
			WHERE
				x.ref_doc_no = o.ref_doc_no
				AND x.action = 'PRINT')
			AND r.sampleid = o.ref_doc_no
							 ) AS print_date,

							(
		SELECT
			DISTINCT o.createddatetime AS printing_time
		FROM
			PRHLIVE.print_audit_details o
		WHERE
			o.createddatetime =(
			SELECT
				min(x.createddatetime)
			FROM
				PRHLIVE.print_audit_details x
			WHERE
				x.ref_doc_no = o.ref_doc_no
				AND x.action = 'EMAIL')
			AND r.sampleid = o.ref_doc_no
			AND r.lab_report_mail_send = 1
							) AS email_date,


							spd.profiledesc patient_type,
							decode(patreg.registration_type, 666337, 'Corporate', 'Non-Corporate' ) registration_type
	FROM
		PRHLIVE.LABORDER t
	LEFT OUTER JOIN PRHLIVE.LABRESULT r
		 ON
		t.lab_order_id = r.lab_order_id
	LEFT OUTER JOIN PRHLIVE.LABSERVICEMASTER l
		 ON
		t.lab_service_id = l.lab_service_id
	LEFT OUTER JOIN PRHLIVE.SERVICELOCATIONMAP s
		 ON
		l.service_id = s.service_id
	LEFT OUTER JOIN PRHLIVE.SERVICECENTER c
		 ON
		s.service_center_id = c.service_center_id
	LEFT OUTER JOIN PRHLIVE.turnaroundtime tat
		 ON
		l.lab_service_id = tat.labservicemaster
	LEFT JOIN PRHLIVE.PATIENT p
		 ON
		t.patient_id = p.patient_id
	LEFT JOIN PRHLIVE.patientregistration patreg
		 ON
		p.patient_id = patreg.patient_id
	LEFT JOIN PRHLIVE.simpleprofiledata spd
		 ON
		patreg.patient_type = spd.id
	LEFT JOIN PRHLIVE.serviceorder so
		 ON
		t.service_order_id = so.id
	LEFT JOIN PRHLIVE.orders ord
		 ON
		so.orders_id = ord.order_id
	WHERE
		1 = 1
		--AND l.lab_service_name = '% Saturation (Transferrin)'
		AND r.certified_date IS NOT NULL
		--and p.patientname not like UPPER('%Dummy%')
		AND trunc(r.certified_date) = trunc(sysdate-1)
		and  ROWNUM <= 100
				''',ict_conn)

		df_his = df_his.rename(columns=str.lower)
		# print('his Lab tat', df_his.columns)

		return df_his

def tat_data(conn1,cur1):
	df_dwh = pd.read_sql('''
		SELECT 
	s.service_id AS tat_service_id,
	s.service_name,
	s.test_type,
	s.start_time,
	s.end_time,
	s.days,
	s.report_delivery,
	s.status,
	SUBSTRING(s.start_time FROM 1 FOR 2)::integer AS start_time_hour,
	SUBSTRING(s.start_time FROM 4 FOR 2)::integer AS start_time_min,
	SUBSTRING(s.end_time FROM 1 FOR 2)::integer AS end_time_hour,
	SUBSTRING(s.end_time FROM 4 FOR 2)::integer AS end_time_min,
	SUBSTRING(s.report_delivery FROM 1 FOR 2)::integer AS report_delivery_time_hour,
	SUBSTRING(s.report_delivery FROM 4 FOR 2)::integer AS report_delivery_time_min
FROM tat_times.lab_tat_tat_time_lab s;


	''', conn1)
	df_dwh = df_dwh.rename(columns=str.lower)
	print(df_dwh)
	# print('DWH Lab tat', df_dwh.columns)
	return df_dwh

def Tat(df_his, df_dwh):
	data1 = df_his.merge(df_dwh, left_on='service_id', right_on='tat_service_id', how='left')
	#data1['tat_code'] = data1['tat_code'].fillna(0)
	data = data1[['lab_order_id', 'mrno', 'patientname', 'age', 'age_unit', 'gender', 'mobileno', 'email',
					'lab_service_name', 'lab_service_id', 'profile_id', 'service_id', 'tat_service_id',
					'service_center_name', 'ordered_date', 'samplecollected_date', 'samplegenerated_date',
					'a_date_str', 'certified_date', 'updateddatetime', 'print_date', 'email_date', 'patient_type',
					'registration_type', 'test_type']]
	
	data['ideal_time'] = pd.Series()
	data['status'] = pd.Series()
	data.to_csv('data.csv', index=False)
	print(data)


	for index, row_dwh in df_dwh.iterrows():
		for index1, row_ict in df_his.iterrows():  # Iterate through df_his instead of df_dwh
			if row_dwh['tat_service_id'] == row_ict['service_id']:  # Use 'tat_service_id' and 'service_id'
				a_date = datetime.strptime(row_ict['a_date_str'], '%Y-%m-%d %H:%M')
				a1 = datetime.strptime(row_dwh['start_time'], "%H:%M").time()
				a2 = datetime.strptime(row_dwh['end_time'], "%H:%M").time()
				if row_dwh['days'] != '0':
					lab_ideal_time1 = a_date.replace(hour=row_dwh['report_delivery_time_hour'],
												   minute=row_dwh['report_delivery_time_min'])
					print('same days data', lab_ideal_time1)
				elif row_dwh['days'] == '0':
					lab_ideal_time1 = (a_date + timedelta(days=row_dwh['days'])).replace(
						hour=row_dwh['report_delivery_time_hour'], minute=row_dwh['report_delivery_time_min'])
					print('same days data', lab_ideal_time1)
				else:
					lab_ideal_time1 = pd.NaT
					
				for index2, row_data in data.iterrows():
					#
					
					
					_email = row_data['email_date']
					
					lab_ideal_time = lab_ideal_time1
					data.at[index, 'ideal_time'] = lab_ideal_time

					if lab_ideal_time > _email:
						data.at[index, 'status'] = 'On Time'
					elif lab_ideal_time < _email:
						data.at[index, 'status'] = 'Late'
					else:
						data.at[index,'status'] = 'None'
				#data = data.rename(columns={'a_date_str': 'sampleaccepted_date'})
				data.to_csv('data1.csv', index=False)

if __name__ == '__main__':
	ict_conn,ict_cursor,conn1,cur1 = db()
	df_his = labdata1(ict_conn, ict_cursor)
	df_dwh = tat_data(conn1,cur1)
	lab_ideal_time = Tat(df_his,df_dwh)
	

