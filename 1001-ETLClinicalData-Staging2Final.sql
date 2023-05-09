-------------------------------------------------------------------------
----- FUNCTION: staging.etlstaging2final()  This function will move -----
--       the approved and validated data to the final ODS schema       --
-------------------------------------------------------------------------

DROP FUNCTION IF EXISTS staging.etlstaging2final;

CREATE OR REPLACE FUNCTION staging.etlstaging2final(
	varYear float, varClinicID uuid
	)

/*
    RETURNS TABLE(
	successfulexecution boolean
	,executiondetails text) 
*/
	RETURNS VOID
    LANGUAGE 'plpgsql'

    --COST 100
    VOLATILE 
    --ROWS 1000
AS $BODY$
DECLARE	

	prelimExecutionSuccessFlag BOOLEAN = FALSE;
	prelimExecutionDetails TEXT = 'Execution Successful!!';
	processName VARCHAR = 'Clinical Data ETL';
	processedRecordCount INT = 0;
	detailStatementDelimiter VARCHAR = E'\r\n\t'; 
	
	exceptionMessageText TEXT;
	exceptionSQLState TEXT;
	exceptionTableName TEXT;
	exceptionColumnName TEXT;
	exceptionDetail TEXT;
	cleanup text;




	
BEGIN 
	--Intro
	RAISE NOTICE 'Executing Migration to FINAL...';
	RAISE NOTICE 'Processing Subject Area: Clinic Data Tables...';
	
	
	--Prepare Clinic Data to be written to the ODS.	
	
	
	RAISE NOTICE '...CLINIC Table';
			
		WITH insertclinic AS (
			INSERT INTO final.clinic (
				clinicid
				, year
				, clinicname 
				, address1 
				, address2 
				, city 
				, state 
				, zip 
				, countyid 
				, maddress1 
				, maddress2 
				, mcity 
				, mstate 
				, mzip 
				, phone 
				, fax 
				, email 
				, website 
				, readonly
				, readonlydt
				, duauploaddt
				, expired
				, expired_bywhom
				, data_extension
				, status 
				, created 
				, created_bywhom 
				, last_modified 
				, last_modified_bywhom 
			)
				  
			SELECT clinicid
				, year
				, clinicname 
				, address1 
				, address2 
				, city 
				, state 
				, zip 
				, countyid 
				, maddress1 
				, maddress2 
				, mcity 
				, mstate 
				, mzip 
				, phone 
				, fax 
				, email 
				, website 
				, readonly
				, readonlydt
				, duauploaddt
				, expired
				, expired_bywhom
				, data_extension
				, status 
				, created 
				, created_bywhom 
				, last_modified 
				, last_modified_bywhom
				FROM staging.clinic
				WHERE year = varyear
				AND clinicid = varclinicid
				
				
				ON CONFLICT (clinicid) 
				DO UPDATE
					SET
						 year = excluded.year
						, clinicname = excluded.clinicname
						, address1 = excluded.address1
						, address2 = excluded.address2
						, city = excluded.city
						, state = excluded.state
						, zip = excluded.zip
						, countyid = excluded.countyid
						, maddress1 = excluded.maddress1
						, maddress2 = excluded.maddress2
						, mcity = excluded.mcity
						, mstate = excluded.mstate
						, mzip = excluded.mzip
						, phone = excluded.phone
						, fax = excluded.fax
						, email = excluded.email
						, website = excluded.website
						, readonly = excluded.readonly
						, readonlydt = excluded.readonlydt
						, duauploaddt = excluded.duauploaddt
						, expired = excluded.expired
						, expired_bywhom = excluded.expired_bywhom
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						clinic.last_modified < excluded.last_modified
					RETURNING clinic.clinicid
		)
						
				SELECT count(*)
				FROM insertclinic
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: CLINIC'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;
	
	RAISE NOTICE '...CLINICCONTACT Table';
		
		WITH insertcliniccontact as (
			INSERT INTO final.cliniccontact (
				cliniccontactid
				, clinicid
				, year
				, personid
				, title
				, phone
				, extension
				, fax
				, email
				, expired
				, expired_reason
				, expired_bywhom
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom
			)
			
			SELECT
				cliniccontactid
				, clinicid
				, year
				, personid
				, title
				, phone
				, extension
				, fax
				, email
				, expired
				, expired_reason
				, expired_bywhom
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom
			FROM staging.cliniccontact
			WHERE year = varyear
			AND clinicid = varclinicid
		
			
			ON CONFLICT (cliniccontactid) 
				DO UPDATE
					SET
						 clinicid = excluded.clinicid
						, year = excluded.year
						, personid = excluded.personid
						, title = excluded.title
						, phone = excluded.phone
						, extension = excluded.extension
						, fax = excluded.fax
						, email = excluded.email
						, expired = excluded.expired
						, expired_reason = excluded.expired_reason
						, expired_bywhom = excluded.expired_bywhom
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						cliniccontact.last_modified < excluded.last_modified
					RETURNING cliniccontact.cliniccontactid
		)	
			
			SELECT count(*)
				FROM insertcliniccontact
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: CLINICCONTACT'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...DESIGANDMEMBER Table';	
		
		WITH insertdesigandmember AS (
			INSERT INTO final.desigandmember (
				year
				, clinicid
				, desig_dental
				, desig_fqhc
				, desig_fqhc_lal
				, desig_freeclinic
				, desig_lhd
				, desig_pcc
				, desig_rhc
				, fqhcdate
				, fqhc_laldate
				, member
				, stategrantee
				, includeinaggregate
				, allowadmin2
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
			)
			
			SELECT 
				year
				, clinicid
				, desig_dental
				, desig_fqhc
				, desig_fqhc_lal
				, desig_freeclinic
				, desig_lhd
				, desig_pcc
				, desig_rhc
				, fqhcdate
				, fqhc_laldate
				, member
				, stategrantee
				, includeinaggregate
				, allowadmin2
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
			FROM staging.desigandmember
			WHERE clinicid = varclinicid
			AND year = varyear
		
			
			ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 desig_dental = excluded.desig_dental
						, desig_fqhc = excluded.desig_fqhc
						, desig_fqhc_lal = excluded.desig_fqhc_lal
						, desig_freeclinic = excluded.desig_freeclinic
						, desig_lhd = excluded.desig_lhd
						, desig_pcc = excluded.desig_pcc
						, desig_rhc = excluded.desig_rhc
						, fqhcdate = excluded.fqhcdate
						, fqhc_laldate = excluded.fqhc_laldate
						, member = excluded.member
						, stategrantee = excluded.stategrantee
						, includeinaggregate = excluded.includeinaggregate
						, allowadmin2 = excluded.allowadmin2
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
					WHERE
						desigandmember.last_modified < excluded.last_modified
					RETURNING desigandmember.clinicid
			
		)	
			
			SELECT count(*)
				FROM insertdesigandmember
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: DESIGANDMEMBER'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;
			
	
	RAISE NOTICE 'Processing Subject Area: Patient Data Tables...';
	RAISE NOTICE '...PATIENTSBYINCOME Table';	
		
		WITH insertpatientsbyincome AS (
			INSERT INTO final.patientsbyincome (
				year
				, clinicid
				, lessthan100fpl
				, lessthan100fpl_nanr
				, _101to150fpl
				, _101to150fpl_nanr
				, _151to200fpl
				, _151to200fpl_nanr
				, greaterthan200fpl
				, greaterthan200fpl_nanr
				, unknownfpl
				, unknownfpl_nanr
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom	
			)				  
				SELECT
					year
					, clinicid
					, lessthan100fpl
					, lessthan100fpl_nanr
					, _101to150fpl
					, _101to150fpl_nanr
					, _151to200fpl
					, _151to200fpl_nanr
					, greaterthan200fpl
					, greaterthan200fpl_nanr
					, unknownfpl
					, unknownfpl_nanr
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.patientsbyincome
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 lessthan100fpl = excluded.lessthan100fpl
						, lessthan100fpl_nanr = excluded.lessthan100fpl_nanr
						, _101to150fpl = excluded._101to150fpl
						, _101to150fpl_nanr = excluded._101to150fpl_nanr
						, _151to200fpl = excluded._151to200fpl
						, _151to200fpl_nanr = excluded._151to200fpl_nanr
						, greaterthan200fpl = excluded.greaterthan200fpl
						, greaterthan200fpl_nanr = excluded.greaterthan200fpl_nanr
						, unknownfpl = excluded.unknownfpl
						, unknownfpl_nanr = excluded.unknownfpl_nanr
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						patientsbyincome.last_modified < excluded.last_modified 
					RETURNING patientsbyincome.clinicid
		)
						
				SELECT count(*)
				FROM insertpatientsbyincome
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: PATIENTSBYINCOME'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...PATIENTSBYPAYORTYPE Table';	
		
		WITH insertpatientsbypayortype AS (
			INSERT INTO final.patientsbypayortype (
				year
				, clinicid
				, medicare
				, medicare_nanr
				, medicaid
				, medicaid_nanr
				, chip
				, chip_nanr
				, otherpublic
				, otherpublic_nanr
				, privateins
				, privateins_nanr
				, uninsured
				, uninsured_nanr
				, unknowninsstatus
				, unknowninsstatus_nanr
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 	
			)
				  
				SELECT
					year
					, clinicid
					, medicare
					, medicare_nanr
					, medicaid
					, medicaid_nanr
					, chip
					, chip_nanr
					, otherpublic
					, otherpublic_nanr
					, privateins
					, privateins_nanr
					, uninsured
					, uninsured_nanr
					, unknowninsstatus
					, unknowninsstatus_nanr
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.patientsbypayortype
					WHERE year = varyear
					AND clinicid = varclinicid
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 medicare = excluded.medicare
						, medicare_nanr = excluded.medicare_nanr
						, medicaid = excluded.medicaid
						, medicaid_nanr = excluded.medicaid_nanr
						, chip = excluded.chip
						, chip_nanr = excluded.chip_nanr
						, otherpublic = excluded.otherpublic
						, otherpublic_nanr = excluded.otherpublic_nanr
						, privateins = excluded.privateins
						, privateins_nanr = excluded.privateins_nanr
						, uninsured = excluded.uninsured
						, uninsured_nanr = excluded.uninsured_nanr
						, unknowninsstatus = excluded.unknowninsstatus
						, unknowninsstatus_nanr = excluded.unknowninsstatus_nanr
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						patientsbypayortype.last_modified < excluded.last_modified
					RETURNING patientsbypayortype.clinicid
		)
						
				SELECT count(*)
				FROM insertpatientsbypayortype
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: PATIENTSBYPAYORTYPE'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...PATIENTSBYSPECIALPOPS Table';	
		
		WITH insertpatientsbyspecialpops AS (
			INSERT INTO final.patientsbyspecialpops (
				year
				, clinicid
				, agworkers
				, homeless
				, schoolbased
				, veterans
				, publichouse
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT 
					year
					, clinicid
					, agworkers
					, homeless
					, schoolbased
					, veterans
					, publichouse
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.patientsbyspecialpops
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 agworkers = excluded.agworkers
						, homeless = excluded.homeless
						, schoolbased = excluded.schoolbased
						, veterans = excluded.veterans
						, publichouse = excluded.publichouse
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						patientsbyspecialpops.last_modified < excluded.last_modified
					RETURNING patientsbyspecialpops.clinicid
		)
						
				SELECT count(*)
				FROM insertpatientsbyspecialpops
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: PATIENTSBYSPECIALPOPS'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...PATIENTSBYSPECIALVULNERABLEPOPS Table';	
		
		WITH insertpatientsbyspecialvulnerablepops AS (
			INSERT INTO final.patientsbyspecialvulnerablepops (
				 year
				, clinicid
				, agworkers
				, agworkers_nanr
				, homeless
				, homeless_nanr
				, schoolbased
				, schoolbased_nanr
				, veterans
				, veterans_nanr
				, publichouse
				, publichouse_nanr
				, asian
				, asian_hispanic
				, asian_nonhispanic
				, asian_unreported
				, asian_nanr
				, gay
				, gay_nanr
				, schoolage
				, schoolage_nanr
				, medlegpartner
				, medlegpartner_nanr
				, lowincome
				, lowincome_nanr
				, nativehawaiian
				, nativehawaiian_hispanic
				, nativehawaiian_nonhispanic
				, nativehawaiian_unreported
				, nativehawaiian_nanr
				, otherpacificislander
				, otherpacificislander_hispanic
				, otherpacificislander_nonhispanic
				, otherpacificislander_unreported
				, otherpacificislander_nanr
				, black
				, black_hispanic
				, black_nonhispanic
				, black_unreported
				, black_nanr
				, amindian
				, amindian_hispanic
				, amindian_nonhispanic
				, amindian_unreported
				, amindian_nanr
				, white
				, white_hispanic
				, white_nonhispanic
				, white_unreported
				, white_nanr
				, multiplerace
				, multiplerace_hispanic
				, multiplerace_nonhispanic
				, multiplerace_unreported
				, multiplerace_nanr
				, unreportedrace
				, unreportedrace_hispanic
				, unreportedrace_nonhispanic
				, unreportedrace_unreported
				, unreportedrace_nanr
				, noenglish
				, noenglish_nanr
				, straight
				, straight_nanr
				, bisexual
				, bisexual_nanr
				, otherso
				, otherso_nanr
				, unknownso
				, unknownso_nanr
				, unreportedso
				, unreportedso_nanr
				, unknown2so
				, unknown2so_nanr
				, male
				, male_nanr
				, female
				, female_nanr
				, transmale
				, transmale_nanr
				, transfemale
				, transfemale_nanr
				, queer
				, queer_nanr
				, othergi
				, othergi_nanr
				, unreportedgi
				, unreportedgi_nanr
				, unknowngi
				, unknowngi_nanr
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					 year
					, clinicid
					, agworkers
					, agworkers_nanr
					, homeless
					, homeless_nanr
					, schoolbased
					, schoolbased_nanr
					, veterans
					, veterans_nanr
					, publichouse
					, publichouse_nanr
					, asian
					, asian_hispanic
					, asian_nonhispanic
					, asian_unreported
					, asian_nanr
					, gay
					, gay_nanr
					, schoolage
					, schoolage_nanr
					, medlegpartner
					, medlegpartner_nanr
					, lowincome
					, lowincome_nanr
					, nativehawaiian
					, nativehawaiian_hispanic
					, nativehawaiian_nonhispanic
					, nativehawaiian_unreported
					, nativehawaiian_nanr
					, otherpacificislander
					, otherpacificislander_hispanic
					, otherpacificislander_nonhispanic
					, otherpacificislander_unreported
					, otherpacificislander_nanr
					, black
					, black_hispanic
					, black_nonhispanic
					, black_unreported
					, black_nanr
					, amindian
					, amindian_hispanic
					, amindian_nonhispanic
					, amindian_unreported
					, amindian_nanr
					, white
					, white_hispanic
					, white_nonhispanic
					, white_unreported
					, white_nanr
					, multiplerace
					, multiplerace_hispanic
					, multiplerace_nonhispanic
					, multiplerace_unreported
					, multiplerace_nanr
					, unreportedrace
					, unreportedrace_hispanic
					, unreportedrace_nonhispanic
					, unreportedrace_unreported
					, unreportedrace_nanr
					, noenglish
					, noenglish_nanr
					, straight
					, straight_nanr
					, bisexual
					, bisexual_nanr
					, otherso
					, otherso_nanr
					, unknownso
					, unknownso_nanr
					, unreportedso
					, unreportedso_nanr
					, unknown2so
					, unknown2so_nanr
					, male
					, male_nanr
					, female
					, female_nanr
					, transmale
					, transmale_nanr
					, transfemale
					, transfemale_nanr
					, queer
					, queer_nanr
					, othergi
					, othergi_nanr
					, unreportedgi
					, unreportedgi_nanr
					, unknowngi
					, unknowngi_nanr
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.patientsbyspecialvulnerablepops
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 agworkers = excluded.agworkers
						, agworkers_nanr = excluded.agworkers_nanr
						, homeless = excluded.homeless
						, homeless_nanr = excluded.homeless_nanr
						, schoolbased = excluded.schoolbased
						, schoolbased_nanr = excluded.schoolbased_nanr
						, veterans = excluded.veterans
						, veterans_nanr = excluded.veterans_nanr
						, publichouse = excluded.publichouse
						, publichouse_nanr = excluded.publichouse_nanr
						, asian = excluded.asian
						, asian_hispanic = excluded.asian_hispanic
						, asian_nonhispanic = excluded.asian_nonhispanic
						, asian_unreported = excluded.asian_unreported
						, asian_nanr = excluded.asian_nanr
						, gay = excluded.gay
						, gay_nanr = excluded.gay_nanr
						, schoolage = excluded.schoolage
						, schoolage_nanr = excluded.schoolage_nanr
						, medlegpartner = excluded.medlegpartner
						, medlegpartner_nanr = excluded.medlegpartner_nanr
						, lowincome = excluded.lowincome
						, lowincome_nanr = excluded.lowincome_nanr
						, nativehawaiian = excluded.nativehawaiian
						, nativehawaiian_hispanic = excluded.nativehawaiian_hispanic
						, nativehawaiian_nonhispanic = excluded.nativehawaiian_nonhispanic
						, nativehawaiian_unreported = excluded.nativehawaiian_unreported
						, nativehawaiian_nanr = excluded.nativehawaiian_nanr
						, otherpacificislander = excluded.otherpacificislander
						, otherpacificislander_hispanic = excluded.otherpacificislander_hispanic
						, otherpacificislander_nonhispanic = excluded.otherpacificislander_nonhispanic
						, otherpacificislander_unreported = excluded.otherpacificislander_unreported
						, otherpacificislander_nanr = excluded.otherpacificislander_nanr
						, black = excluded.black
						, black_hispanic = excluded.black_hispanic
						, black_nonhispanic = excluded.black_nonhispanic
						, black_unreported = excluded.black_unreported
						, black_nanr = excluded.black_nanr
						, amindian = excluded.amindian
						, amindian_hispanic = excluded.amindian_hispanic
						, amindian_nonhispanic = excluded.amindian_nonhispanic
						, amindian_unreported = excluded.amindian_unreported
						, amindian_nanr = excluded.amindian_nanr
						, white = excluded.white
						, white_hispanic = excluded.white_hispanic
						, white_nonhispanic = excluded.white_nonhispanic
						, white_unreported = excluded.white_unreported
						, white_nanr = excluded.white_nanr
						, multiplerace = excluded.multiplerace
						, multiplerace_hispanic = excluded.multiplerace_hispanic
						, multiplerace_nonhispanic = excluded.multiplerace_nonhispanic
						, multiplerace_unreported = excluded.multiplerace_unreported
						, multiplerace_nanr = excluded.multiplerace_nanr
						, unreportedrace = excluded.unreportedrace
						, unreportedrace_hispanic = excluded.unreportedrace_hispanic
						, unreportedrace_nonhispanic = excluded.unreportedrace_nonhispanic
						, unreportedrace_unreported = excluded.unreportedrace_unreported
						, unreportedrace_nanr = excluded.unreportedrace_nanr
						, noenglish = excluded.noenglish
						, noenglish_nanr = excluded.noenglish_nanr
						, straight = excluded.straight
						, straight_nanr = excluded.straight_nanr
						, bisexual = excluded.bisexual
						, bisexual_nanr = excluded.bisexual_nanr
						, otherso = excluded.otherso
						, otherso_nanr = excluded.otherso_nanr
						, unknownso = excluded.unknownso
						, unknownso_nanr = excluded.unknownso_nanr
						, unreportedso = excluded.unreportedso
						, unreportedso_nanr = excluded.unreportedso_nanr
						, unknown2so = excluded.unknown2so
						, unknown2so_nanr = excluded.unknown2so_nanr
						, male = excluded.male
						, male_nanr = excluded.male_nanr
						, female = excluded.female
						, female_nanr = excluded.female_nanr
						, transmale = excluded.transmale
						, transmale_nanr = excluded.transmale_nanr
						, transfemale = excluded.transfemale
						, transfemale_nanr = excluded.transfemale_nanr
						, queer = excluded.queer
						, queer_nanr = excluded.queer_nanr
						, othergi = excluded.othergi
						, othergi_nanr = excluded.othergi_nanr
						, unreportedgi = excluded.unreportedgi
						, unreportedgi_nanr = excluded.unreportedgi_nanr
						, unknowngi = excluded.unknowngi
						, unknowngi_nanr = excluded.unknowngi_nanr
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						patientsbyspecialvulnerablepops.last_modified < excluded.last_modified
					RETURNING patientsbyspecialvulnerablepops.clinicid
		)
						
				SELECT count(*)
				FROM insertpatientsbyspecialvulnerablepops
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: PATIENTSBYSPECIALVULNERABLEPOPS'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...PATIENTSPRELIMDATA Table';	
		
		WITH insertpatientsprelimdata AS (
			INSERT INTO final.patientsprelimdata (
				 year
				, clinicid
				, totalpatients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom
			)
				  
				SELECT
					 year
					, clinicid
					, totalpatients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.patientsprelimdata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 totalpatients = excluded.totalpatients
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						patientsprelimdata.last_modified < excluded.last_modified
					RETURNING patientsprelimdata.clinicid
		)
						
				SELECT count(*)
				FROM insertpatientsprelimdata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: PATIENTSPRELIMDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...SLIDINGFEEDATA Table';	
		
		WITH insertslidingfeedata AS (
			INSERT INTO final.slidingfeedata (
				 year
				, clinicid
				, numpatients
				, discounttotal
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT
					 year
					, clinicid
					, numpatients
					, discounttotal
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.slidingfeedata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 numpatients = excluded.numpatients
						, discounttotal = excluded.discounttotal
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						slidingfeedata.last_modified < excluded.last_modified
					RETURNING slidingfeedata.clinicid
		)
						
				SELECT count(*)
				FROM insertslidingfeedata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: SLIDINGFEEDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	
	RAISE NOTICE '...STAFFINGPRELIMDATA Table';	
		
		WITH insertstaffingprelimdata AS (
			INSERT INTO final.staffingprelimdata (
				 year
				, clinicid
				, totalvisits
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT
					 year
					, clinicid
					, totalvisits
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.staffingprelimdata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 totalvisits = excluded.totalvisits
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						staffingprelimdata.last_modified < excluded.last_modified
					RETURNING staffingprelimdata.clinicid
		)
						
				SELECT count(*)
				FROM insertstaffingprelimdata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: STAFFINGPRELIMDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


		RAISE NOTICE 'Processing Subject Area: Staffing and Utilization...';
		RAISE NOTICE '...ADMINANDFACILITY Table';	
		
		WITH insertadminandfacility AS (
			INSERT INTO final.adminandfacility (
				 year
				, clinicid
				, qipersonnel_paidprovfte
				, qipersonnel_volunteers
				, qipersonnel_volhours
				, mgmtandsupportstaff_paidprovfte
				, mgmtandsupportstaff_volunteers
				, mgmtandsupportstaff_volhours
				, fiscalandbillingstaff_paidprovfte
				, fiscalandbillingstaff_volunteers
				, fiscalandbillingstaff_volhours
				, itstaff_paidprovfte
				, itstaff_volunteers
				, itstaff_volhours
				, facilitystaff_paidprovfte
				, facilitystaff_volunteers
				, facilitystaff_volhours
				, patientsupportstaff_paidprovfte
				, patientsupportstaff_volunteers
				, patientsupportstaff_volhours
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					 year
					, clinicid
					, qipersonnel_paidprovfte
					, qipersonnel_volunteers
					, qipersonnel_volhours
					, mgmtandsupportstaff_paidprovfte
					, mgmtandsupportstaff_volunteers
					, mgmtandsupportstaff_volhours
					, fiscalandbillingstaff_paidprovfte
					, fiscalandbillingstaff_volunteers
					, fiscalandbillingstaff_volhours
					, itstaff_paidprovfte
					, itstaff_volunteers
					, itstaff_volhours
					, facilitystaff_paidprovfte
					, facilitystaff_volunteers
					, facilitystaff_volhours
					, patientsupportstaff_paidprovfte
					, patientsupportstaff_volunteers
					, patientsupportstaff_volhours
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.adminandfacility
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 qipersonnel_paidprovfte = excluded.qipersonnel_paidprovfte
						, qipersonnel_volunteers = excluded.qipersonnel_volunteers
						, qipersonnel_volhours = excluded.qipersonnel_volhours
						, mgmtandsupportstaff_paidprovfte = excluded.mgmtandsupportstaff_paidprovfte
						, mgmtandsupportstaff_volunteers = excluded.mgmtandsupportstaff_volunteers
						, mgmtandsupportstaff_volhours = excluded.mgmtandsupportstaff_volhours
						, fiscalandbillingstaff_paidprovfte = excluded.fiscalandbillingstaff_paidprovfte
						, fiscalandbillingstaff_volunteers = excluded.fiscalandbillingstaff_volunteers
						, fiscalandbillingstaff_volhours = excluded.fiscalandbillingstaff_volhours
						, itstaff_paidprovfte = excluded.itstaff_paidprovfte
						, itstaff_volunteers = excluded.itstaff_volunteers
						, itstaff_volhours = excluded.itstaff_volhours
						, facilitystaff_paidprovfte = excluded.facilitystaff_paidprovfte
						, facilitystaff_volunteers = excluded.facilitystaff_volunteers
						, facilitystaff_volhours = excluded.facilitystaff_volhours
						, patientsupportstaff_paidprovfte = excluded.patientsupportstaff_paidprovfte
						, patientsupportstaff_volunteers = excluded.patientsupportstaff_volunteers
						, patientsupportstaff_volhours = excluded.patientsupportstaff_volhours
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						adminandfacility.last_modified < excluded.last_modified
					RETURNING adminandfacility.clinicid
		)
						
				SELECT count(*)
				FROM insertadminandfacility
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: ADMINANDFACILITY'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...BEHAVIORALHEALTH Table';	
		
		WITH insertbehavioralhealth AS (
			INSERT INTO final.behavioralhealth (
				 year
				, clinicid
				, mh_psychiatrists_paidprovfte
				, mh_psychiatrists_volunteers
				, mh_psychiatrists_volhours
				, mh_psychiatrists_paidvisits
				, mh_psychiatrists_volvisits
				, mh_psychiatrists_virtualvisits
				, mh_psychiatrists_patients
				, mh_licpsychologists_paidprovfte
				, mh_licpsychologists_volunteers
				, mh_licpsychologists_volhours
				, mh_licpsychologists_paidvisits
				, mh_licpsychologists_volvisits
				, mh_licpsychologists_virtualvisits
				, mh_licpsychologists_patients
				, mh_licsocialworkers_paidprovfte
				, mh_licsocialworkers_volunteers
				, mh_licsocialworkers_volhours
				, mh_licsocialworkers_paidvisits
				, mh_licsocialworkers_volvisits
				, mh_licsocialworkers_virtualvisits
				, mh_licsocialworkers_patients
				, mh_otherlicmhprov_paidprovfte
				, mh_otherlicmhprov_volunteers
				, mh_otherlicmhprov_volhours
				, mh_otherlicmhprov_paidvisits
				, mh_otherlicmhprov_volvisits
				, mh_otherlicmhprov_virtualvisits
				, mh_otherlicmhprov_patients
				, mh_othermhpersonnel_paidprovfte
				, mh_othermhpersonnel_volunteers
				, mh_othermhpersonnel_volhours
				, mh_othermhpersonnel_paidvisits
				, mh_othermhpersonnel_volvisits
				, mh_othermhpersonnel_virtualvisits
				, mh_othermhpersonnel_patients
				, mh_physicians_paidprovfte
				, mh_physicians_volunteers
				, mh_physicians_volhours
				, mh_physicians_paidvisits
				, mh_physicians_volvisits
				, mh_physicians_virtualvisits
				, mh_physicians_patients
				, mh_np_paidprovfte
				, mh_np_volunteers
				, mh_np_volhours
				, mh_np_paidvisits
				, mh_np_volvisits
				, mh_np_virtualvisits
				, mh_np_patients
				, mh_pa_paidprovfte
				, mh_pa_volunteers
				, mh_pa_volhours
				, mh_pa_paidvisits
				, mh_pa_volvisits
				, mh_pa_virtualvisits
				, mh_pa_patients
				, mh_cnm_paidprovfte
				, mh_cnm_volunteers
				, mh_cnm_volhours
				, mh_cnm_paidvisits
				, mh_cnm_volvisits
				, mh_cnm_virtualvisits
				, mh_cnm_patients
				, mh_cns_paidprovfte
				, mh_cns_volunteers
				, mh_cns_volhours
				, mh_cns_paidvisits
				, mh_cns_volvisits
				, mh_cns_virtualvisits
				, mh_cns_patients
				, mh_patients
				, sud_psychiatrists_paidprovfte
				, sud_psychiatrists_volunteers
				, sud_psychiatrists_volhours
				, sud_psychiatrists_paidvisits
				, sud_psychiatrists_volvisits
				, sud_psychiatrists_virtualvisits
				, sud_psychiatrists_patients
				, sud_physicians_paidprovfte
				, sud_physicians_volunteers
				, sud_physicians_volhours
				, sud_physicians_paidvisits
				, sud_physicians_volvisits
				, sud_physicians_virtualvisits
				, sud_physicians_patients
				, sud_np_paidprovfte
				, sud_np_volunteers
				, sud_np_volhours
				, sud_np_paidvisits
				, sud_np_volvisits
				, sud_np_virtualvisits
				, sud_np_patients
				, sud_pa_paidprovfte
				, sud_pa_volunteers
				, sud_pa_volhours
				, sud_pa_paidvisits
				, sud_pa_volvisits
				, sud_pa_virtualvisits
				, sud_pa_patients
				, sud_cnm_paidprovfte
				, sud_cnm_volunteers
				, sud_cnm_volhours
				, sud_cnm_paidvisits
				, sud_cnm_volvisits
				, sud_cnm_virtualvisits
				, sud_cnm_patients
				, sud_cns_paidprovfte
				, sud_cns_volunteers
				, sud_cns_volhours
				, sud_cns_paidvisits
				, sud_cns_volvisits
				, sud_cns_virtualvisits
				, sud_cns_patients
				, sud_licpsychologists_paidprovfte
				, sud_licpsychologists_volunteers
				, sud_licpsychologists_volhours
				, sud_licpsychologists_paidvisits
				, sud_licpsychologists_volvisits
				, sud_licpsychologists_virtualvisits
				, sud_licpsychologists_patients
				, sud_licsocialworkers_paidprovfte
				, sud_licsocialworkers_volunteers
				, sud_licsocialworkers_volhours
				, sud_licsocialworkers_paidvisits
				, sud_licsocialworkers_volvisits
				, sud_licsocialworkers_virtualvisits
				, sud_licsocialworkers_patients
				, sud_otherlicsaprov_paidprovfte
				, sud_otherlicsaprov_volunteers
				, sud_otherlicsaprov_volhours
				, sud_otherlicsaprov_paidvisits
				, sud_otherlicsaprov_volvisits
				, sud_othersastaff_paidprovfte
				, sud_othersastaff_volunteers
				, sud_othersastaff_volhours
				, sud_othersastaff_paidvisits
				, sud_othersastaff_volvisits
				, sud_saservices_paidprovfte
				, sud_saservices_volunteers
				, sud_saservices_volhours
				, sud_saservices_paidvisits
				, sud_saservices_volvisits
				, sud_saservices_virtualvisits
				, sud_saservices_patients
				, sud_patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT 
					 year
					, clinicid
					, mh_psychiatrists_paidprovfte
					, mh_psychiatrists_volunteers
					, mh_psychiatrists_volhours
					, mh_psychiatrists_paidvisits
					, mh_psychiatrists_volvisits
					, mh_psychiatrists_virtualvisits
					, mh_psychiatrists_patients
					, mh_licpsychologists_paidprovfte
					, mh_licpsychologists_volunteers
					, mh_licpsychologists_volhours
					, mh_licpsychologists_paidvisits
					, mh_licpsychologists_volvisits
					, mh_licpsychologists_virtualvisits
					, mh_licpsychologists_patients
					, mh_licsocialworkers_paidprovfte
					, mh_licsocialworkers_volunteers
					, mh_licsocialworkers_volhours
					, mh_licsocialworkers_paidvisits
					, mh_licsocialworkers_volvisits
					, mh_licsocialworkers_virtualvisits
					, mh_licsocialworkers_patients
					, mh_otherlicmhprov_paidprovfte
					, mh_otherlicmhprov_volunteers
					, mh_otherlicmhprov_volhours
					, mh_otherlicmhprov_paidvisits
					, mh_otherlicmhprov_volvisits
					, mh_otherlicmhprov_virtualvisits
					, mh_otherlicmhprov_patients
					, mh_othermhpersonnel_paidprovfte
					, mh_othermhpersonnel_volunteers
					, mh_othermhpersonnel_volhours
					, mh_othermhpersonnel_paidvisits
					, mh_othermhpersonnel_volvisits
					, mh_othermhpersonnel_virtualvisits
					, mh_othermhpersonnel_patients
					, mh_physicians_paidprovfte
					, mh_physicians_volunteers
					, mh_physicians_volhours
					, mh_physicians_paidvisits
					, mh_physicians_volvisits
					, mh_physicians_virtualvisits
					, mh_physicians_patients
					, mh_np_paidprovfte
					, mh_np_volunteers
					, mh_np_volhours
					, mh_np_paidvisits
					, mh_np_volvisits
					, mh_np_virtualvisits
					, mh_np_patients
					, mh_pa_paidprovfte
					, mh_pa_volunteers
					, mh_pa_volhours
					, mh_pa_paidvisits
					, mh_pa_volvisits
					, mh_pa_virtualvisits
					, mh_pa_patients
					, mh_cnm_paidprovfte
					, mh_cnm_volunteers
					, mh_cnm_volhours
					, mh_cnm_paidvisits
					, mh_cnm_volvisits
					, mh_cnm_virtualvisits
					, mh_cnm_patients
					, mh_cns_paidprovfte
					, mh_cns_volunteers
					, mh_cns_volhours
					, mh_cns_paidvisits
					, mh_cns_volvisits
					, mh_cns_virtualvisits
					, mh_cns_patients
					, mh_patients
					, sud_psychiatrists_paidprovfte
					, sud_psychiatrists_volunteers
					, sud_psychiatrists_volhours
					, sud_psychiatrists_paidvisits
					, sud_psychiatrists_volvisits
					, sud_psychiatrists_virtualvisits
					, sud_psychiatrists_patients
					, sud_physicians_paidprovfte
					, sud_physicians_volunteers
					, sud_physicians_volhours
					, sud_physicians_paidvisits
					, sud_physicians_volvisits
					, sud_physicians_virtualvisits
					, sud_physicians_patients
					, sud_np_paidprovfte
					, sud_np_volunteers
					, sud_np_volhours
					, sud_np_paidvisits
					, sud_np_volvisits
					, sud_np_virtualvisits
					, sud_np_patients
					, sud_pa_paidprovfte
					, sud_pa_volunteers
					, sud_pa_volhours
					, sud_pa_paidvisits
					, sud_pa_volvisits
					, sud_pa_virtualvisits
					, sud_pa_patients
					, sud_cnm_paidprovfte
					, sud_cnm_volunteers
					, sud_cnm_volhours
					, sud_cnm_paidvisits
					, sud_cnm_volvisits
					, sud_cnm_virtualvisits
					, sud_cnm_patients
					, sud_cns_paidprovfte
					, sud_cns_volunteers
					, sud_cns_volhours
					, sud_cns_paidvisits
					, sud_cns_volvisits
					, sud_cns_virtualvisits
					, sud_cns_patients
					, sud_licpsychologists_paidprovfte
					, sud_licpsychologists_volunteers
					, sud_licpsychologists_volhours
					, sud_licpsychologists_paidvisits
					, sud_licpsychologists_volvisits
					, sud_licpsychologists_virtualvisits
					, sud_licpsychologists_patients
					, sud_licsocialworkers_paidprovfte
					, sud_licsocialworkers_volunteers
					, sud_licsocialworkers_volhours
					, sud_licsocialworkers_paidvisits
					, sud_licsocialworkers_volvisits
					, sud_licsocialworkers_virtualvisits
					, sud_licsocialworkers_patients
					, sud_otherlicsaprov_paidprovfte
					, sud_otherlicsaprov_volunteers
					, sud_otherlicsaprov_volhours
					, sud_otherlicsaprov_paidvisits
					, sud_otherlicsaprov_volvisits
					, sud_othersastaff_paidprovfte
					, sud_othersastaff_volunteers
					, sud_othersastaff_volhours
					, sud_othersastaff_paidvisits
					, sud_othersastaff_volvisits
					, sud_saservices_paidprovfte
					, sud_saservices_volunteers
					, sud_saservices_volhours
					, sud_saservices_paidvisits
					, sud_saservices_volvisits
					, sud_saservices_virtualvisits
					, sud_saservices_patients
					, sud_patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.behavioralhealth
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 mh_psychiatrists_paidprovfte = excluded.mh_psychiatrists_paidprovfte
						, mh_psychiatrists_volunteers = excluded.mh_psychiatrists_volunteers
						, mh_psychiatrists_volhours = excluded.mh_psychiatrists_volhours
						, mh_psychiatrists_paidvisits = excluded.mh_psychiatrists_paidvisits
						, mh_psychiatrists_volvisits = excluded.mh_psychiatrists_volvisits
						, mh_psychiatrists_virtualvisits = excluded.mh_psychiatrists_virtualvisits
						, mh_psychiatrists_patients = excluded.mh_psychiatrists_patients
						, mh_licpsychologists_paidprovfte = excluded.mh_licpsychologists_paidprovfte
						, mh_licpsychologists_volunteers = excluded.mh_licpsychologists_volunteers
						, mh_licpsychologists_volhours = excluded.mh_licpsychologists_volhours
						, mh_licpsychologists_paidvisits = excluded.mh_licpsychologists_paidvisits
						, mh_licpsychologists_volvisits = excluded.mh_licpsychologists_volvisits
						, mh_licpsychologists_virtualvisits = excluded.mh_licpsychologists_virtualvisits
						, mh_licpsychologists_patients = excluded.mh_licpsychologists_patients
						, mh_licsocialworkers_paidprovfte = excluded.mh_licsocialworkers_paidprovfte
						, mh_licsocialworkers_volunteers = excluded.mh_licsocialworkers_volunteers
						, mh_licsocialworkers_volhours = excluded.mh_licsocialworkers_volhours
						, mh_licsocialworkers_paidvisits = excluded.mh_licsocialworkers_paidvisits
						, mh_licsocialworkers_volvisits = excluded.mh_licsocialworkers_volvisits
						, mh_licsocialworkers_virtualvisits = excluded.mh_licsocialworkers_virtualvisits
						, mh_licsocialworkers_patients = excluded.mh_licsocialworkers_patients
						, mh_otherlicmhprov_paidprovfte = excluded.mh_otherlicmhprov_paidprovfte
						, mh_otherlicmhprov_volunteers = excluded.mh_otherlicmhprov_volunteers
						, mh_otherlicmhprov_volhours = excluded.mh_otherlicmhprov_volhours
						, mh_otherlicmhprov_paidvisits = excluded.mh_otherlicmhprov_paidvisits
						, mh_otherlicmhprov_volvisits = excluded.mh_otherlicmhprov_volvisits
						, mh_otherlicmhprov_virtualvisits = excluded.mh_otherlicmhprov_virtualvisits
						, mh_otherlicmhprov_patients = excluded.mh_otherlicmhprov_patients
						, mh_othermhpersonnel_paidprovfte = excluded.mh_othermhpersonnel_paidprovfte
						, mh_othermhpersonnel_volunteers = excluded.mh_othermhpersonnel_volunteers
						, mh_othermhpersonnel_volhours = excluded.mh_othermhpersonnel_volhours
						, mh_othermhpersonnel_paidvisits = excluded.mh_othermhpersonnel_paidvisits
						, mh_othermhpersonnel_volvisits = excluded.mh_othermhpersonnel_volvisits
						, mh_othermhpersonnel_virtualvisits = excluded.mh_othermhpersonnel_virtualvisits
						, mh_othermhpersonnel_patients = excluded.mh_othermhpersonnel_patients
						, mh_physicians_paidprovfte = excluded.mh_physicians_paidprovfte
						, mh_physicians_volunteers = excluded.mh_physicians_volunteers
						, mh_physicians_volhours = excluded.mh_physicians_volhours
						, mh_physicians_paidvisits = excluded.mh_physicians_paidvisits
						, mh_physicians_volvisits = excluded.mh_physicians_volvisits
						, mh_physicians_virtualvisits = excluded.mh_physicians_virtualvisits
						, mh_physicians_patients = excluded.mh_physicians_patients
						, mh_np_paidprovfte = excluded.mh_np_paidprovfte
						, mh_np_volunteers = excluded.mh_np_volunteers
						, mh_np_volhours = excluded.mh_np_volhours
						, mh_np_paidvisits = excluded.mh_np_paidvisits
						, mh_np_volvisits = excluded.mh_np_volvisits
						, mh_np_virtualvisits = excluded.mh_np_virtualvisits
						, mh_np_patients = excluded.mh_np_patients
						, mh_pa_paidprovfte = excluded.mh_pa_paidprovfte
						, mh_pa_volunteers = excluded.mh_pa_volunteers
						, mh_pa_volhours = excluded.mh_pa_volhours
						, mh_pa_paidvisits = excluded.mh_pa_paidvisits
						, mh_pa_volvisits = excluded.mh_pa_volvisits
						, mh_pa_virtualvisits = excluded.mh_pa_virtualvisits
						, mh_pa_patients = excluded.mh_pa_patients
						, mh_cnm_paidprovfte = excluded.mh_cnm_paidprovfte
						, mh_cnm_volunteers = excluded.mh_cnm_volunteers
						, mh_cnm_volhours = excluded.mh_cnm_volhours
						, mh_cnm_paidvisits = excluded.mh_cnm_paidvisits
						, mh_cnm_volvisits = excluded.mh_cnm_volvisits
						, mh_cnm_virtualvisits = excluded.mh_cnm_virtualvisits
						, mh_cnm_patients = excluded.mh_cnm_patients
						, mh_cns_paidprovfte = excluded.mh_cns_paidprovfte
						, mh_cns_volunteers = excluded.mh_cns_volunteers
						, mh_cns_volhours = excluded.mh_cns_volhours
						, mh_cns_paidvisits = excluded.mh_cns_paidvisits
						, mh_cns_volvisits = excluded.mh_cns_volvisits
						, mh_cns_virtualvisits = excluded.mh_cns_virtualvisits
						, mh_cns_patients = excluded.mh_cns_patients
						, mh_patients = excluded.mh_patients
						, sud_psychiatrists_paidprovfte = excluded.sud_psychiatrists_paidprovfte
						, sud_psychiatrists_volunteers = excluded.sud_psychiatrists_volunteers
						, sud_psychiatrists_volhours = excluded.sud_psychiatrists_volhours
						, sud_psychiatrists_paidvisits = excluded.sud_psychiatrists_paidvisits
						, sud_psychiatrists_volvisits = excluded.sud_psychiatrists_volvisits
						, sud_psychiatrists_virtualvisits = excluded.sud_psychiatrists_virtualvisits
						, sud_psychiatrists_patients = excluded.sud_psychiatrists_patients
						, sud_physicians_paidprovfte = excluded.sud_physicians_paidprovfte
						, sud_physicians_volunteers = excluded.sud_physicians_volunteers
						, sud_physicians_volhours = excluded.sud_physicians_volhours
						, sud_physicians_paidvisits = excluded.sud_physicians_paidvisits
						, sud_physicians_volvisits = excluded.sud_physicians_volvisits
						, sud_physicians_virtualvisits = excluded.sud_physicians_virtualvisits
						, sud_physicians_patients = excluded.sud_physicians_patients
						, sud_np_paidprovfte = excluded.sud_np_paidprovfte
						, sud_np_volunteers = excluded.sud_np_volunteers
						, sud_np_volhours = excluded.sud_np_volhours
						, sud_np_paidvisits = excluded.sud_np_paidvisits
						, sud_np_volvisits = excluded.sud_np_volvisits
						, sud_np_virtualvisits = excluded.sud_np_virtualvisits
						, sud_np_patients = excluded.sud_np_patients
						, sud_pa_paidprovfte = excluded.sud_pa_paidprovfte
						, sud_pa_volunteers = excluded.sud_pa_volunteers
						, sud_pa_volhours = excluded.sud_pa_volhours
						, sud_pa_paidvisits = excluded.sud_pa_paidvisits
						, sud_pa_volvisits = excluded.sud_pa_volvisits
						, sud_pa_virtualvisits = excluded.sud_pa_virtualvisits
						, sud_pa_patients = excluded.sud_pa_patients
						, sud_cnm_paidprovfte = excluded.sud_cnm_paidprovfte
						, sud_cnm_volunteers = excluded.sud_cnm_volunteers
						, sud_cnm_volhours = excluded.sud_cnm_volhours
						, sud_cnm_paidvisits = excluded.sud_cnm_paidvisits
						, sud_cnm_volvisits = excluded.sud_cnm_volvisits
						, sud_cnm_virtualvisits = excluded.sud_cnm_virtualvisits
						, sud_cnm_patients = excluded.sud_cnm_patients
						, sud_cns_paidprovfte = excluded.sud_cns_paidprovfte
						, sud_cns_volunteers = excluded.sud_cns_volunteers
						, sud_cns_volhours = excluded.sud_cns_volhours
						, sud_cns_paidvisits = excluded.sud_cns_paidvisits
						, sud_cns_volvisits = excluded.sud_cns_volvisits
						, sud_cns_virtualvisits = excluded.sud_cns_virtualvisits
						, sud_cns_patients = excluded.sud_cns_patients
						, sud_licpsychologists_paidprovfte = excluded.sud_licpsychologists_paidprovfte
						, sud_licpsychologists_volunteers = excluded.sud_licpsychologists_volunteers
						, sud_licpsychologists_volhours = excluded.sud_licpsychologists_volhours
						, sud_licpsychologists_paidvisits = excluded.sud_licpsychologists_paidvisits
						, sud_licpsychologists_volvisits = excluded.sud_licpsychologists_volvisits
						, sud_licpsychologists_virtualvisits = excluded.sud_licpsychologists_virtualvisits
						, sud_licpsychologists_patients = excluded.sud_licpsychologists_patients
						, sud_licsocialworkers_paidprovfte = excluded.sud_licsocialworkers_paidprovfte
						, sud_licsocialworkers_volunteers = excluded.sud_licsocialworkers_volunteers
						, sud_licsocialworkers_volhours = excluded.sud_licsocialworkers_volhours
						, sud_licsocialworkers_paidvisits = excluded.sud_licsocialworkers_paidvisits
						, sud_licsocialworkers_volvisits = excluded.sud_licsocialworkers_volvisits
						, sud_licsocialworkers_virtualvisits = excluded.sud_licsocialworkers_virtualvisits
						, sud_licsocialworkers_patients = excluded.sud_licsocialworkers_patients
						, sud_otherlicsaprov_paidprovfte = excluded.sud_otherlicsaprov_paidprovfte
						, sud_otherlicsaprov_volunteers = excluded.sud_otherlicsaprov_volunteers
						, sud_otherlicsaprov_volhours = excluded.sud_otherlicsaprov_volhours
						, sud_otherlicsaprov_paidvisits = excluded.sud_otherlicsaprov_paidvisits
						, sud_otherlicsaprov_volvisits = excluded.sud_otherlicsaprov_volvisits
						, sud_othersastaff_paidprovfte = excluded.sud_othersastaff_paidprovfte
						, sud_othersastaff_volunteers = excluded.sud_othersastaff_volunteers
						, sud_othersastaff_volhours = excluded.sud_othersastaff_volhours
						, sud_othersastaff_paidvisits = excluded.sud_othersastaff_paidvisits
						, sud_othersastaff_volvisits = excluded.sud_othersastaff_volvisits
						, sud_saservices_paidprovfte = excluded.sud_saservices_paidprovfte
						, sud_saservices_volunteers = excluded.sud_saservices_volunteers
						, sud_saservices_volhours = excluded.sud_saservices_volhours
						, sud_saservices_paidvisits = excluded.sud_saservices_paidvisits
						, sud_saservices_volvisits = excluded.sud_saservices_volvisits
						, sud_saservices_virtualvisits = excluded.sud_saservices_virtualvisits
						, sud_saservices_patients = excluded.sud_saservices_patients
						, sud_patients = excluded.sud_patients
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						behavioralhealth.last_modified < excluded.last_modified
					RETURNING behavioralhealth.clinicid
		)
						
				SELECT count(*)
				FROM insertbehavioralhealth
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: BEHAVIORALHEALTH'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...BH_SERVICEDETAIL Table';	
		
		WITH insertbh_servicedetail AS (
			INSERT INTO final.bh_servicedetail (
				 year
				, clinicid
				, mh_physicians_personnel
				, mh_physicians_clinicvisits
				, mh_physicians_virtualvisits
				, mh_physicians_patients
				, mh_np_personnel
				, mh_np_clinicvisits
				, mh_np_virtualvisits
				, mh_np_patients
				, mh_pa_personnel
				, mh_pa_clinicvisits
				, mh_pa_virtualvisits
				, mh_pa_patients
				, mh_cnm_personnel
				, mh_cnm_clinicvisits
				, mh_cnm_virtualvisits
				, mh_cnm_patients
				, sud_physicians_personnel
				, sud_physicians_clinicvisits
				, sud_physicians_virtualvisits
				, sud_physicians_patients
				, sud_np_personnel
				, sud_np_clinicvisits
				, sud_np_virtualvisits
				, sud_np_patients
				, sud_pa_personnel
				, sud_pa_clinicvisits
				, sud_pa_virtualvisits
				, sud_pa_patients
				, sud_cnm_personnel
				, sud_cnm_clinicvisits
				, sud_cnm_virtualvisits
				, sud_cnm_patients
				, sud_psychiatrists_personnel
				, sud_psychiatrists_clinicvisits
				, sud_psychiatrists_virtualvisits
				, sud_psychiatrists_patients
				, sud_licpsychologists_personnel
				, sud_licpsychologists_clinicvisits
				, sud_licpsychologists_virtualvisits
				, sud_licpsychologists_patients
				, sud_licsocialworkers_personnel
				, sud_licsocialworkers_clinicvisits
				, sud_licsocialworkers_virtualvisits
				, sud_licsocialworkers_patients
				, sud_otherlicmhprov_personnel
				, sud_otherlicmhprov_clinicvisits
				, sud_otherlicmhprov_virtualvisits
				, sud_otherlicmhprov_patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT 	
					 year
					, clinicid
					, mh_physicians_personnel
					, mh_physicians_clinicvisits
					, mh_physicians_virtualvisits
					, mh_physicians_patients
					, mh_np_personnel
					, mh_np_clinicvisits
					, mh_np_virtualvisits
					, mh_np_patients
					, mh_pa_personnel
					, mh_pa_clinicvisits
					, mh_pa_virtualvisits
					, mh_pa_patients
					, mh_cnm_personnel
					, mh_cnm_clinicvisits
					, mh_cnm_virtualvisits
					, mh_cnm_patients
					, sud_physicians_personnel
					, sud_physicians_clinicvisits
					, sud_physicians_virtualvisits
					, sud_physicians_patients
					, sud_np_personnel
					, sud_np_clinicvisits
					, sud_np_virtualvisits
					, sud_np_patients
					, sud_pa_personnel
					, sud_pa_clinicvisits
					, sud_pa_virtualvisits
					, sud_pa_patients
					, sud_cnm_personnel
					, sud_cnm_clinicvisits
					, sud_cnm_virtualvisits
					, sud_cnm_patients
					, sud_psychiatrists_personnel
					, sud_psychiatrists_clinicvisits
					, sud_psychiatrists_virtualvisits
					, sud_psychiatrists_patients
					, sud_licpsychologists_personnel
					, sud_licpsychologists_clinicvisits
					, sud_licpsychologists_virtualvisits
					, sud_licpsychologists_patients
					, sud_licsocialworkers_personnel
					, sud_licsocialworkers_clinicvisits
					, sud_licsocialworkers_virtualvisits
					, sud_licsocialworkers_patients
					, sud_otherlicmhprov_personnel
					, sud_otherlicmhprov_clinicvisits
					, sud_otherlicmhprov_virtualvisits
					, sud_otherlicmhprov_patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.bh_servicedetail
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 mh_physicians_personnel = excluded.mh_physicians_personnel
						, mh_physicians_clinicvisits = excluded.mh_physicians_clinicvisits
						, mh_physicians_virtualvisits = excluded.mh_physicians_virtualvisits
						, mh_physicians_patients = excluded.mh_physicians_patients
						, mh_np_personnel = excluded.mh_np_personnel
						, mh_np_clinicvisits = excluded.mh_np_clinicvisits
						, mh_np_virtualvisits = excluded.mh_np_virtualvisits
						, mh_np_patients = excluded.mh_np_patients
						, mh_pa_personnel = excluded.mh_pa_personnel
						, mh_pa_clinicvisits = excluded.mh_pa_clinicvisits
						, mh_pa_virtualvisits = excluded.mh_pa_virtualvisits
						, mh_pa_patients = excluded.mh_pa_patients
						, mh_cnm_personnel = excluded.mh_cnm_personnel
						, mh_cnm_clinicvisits = excluded.mh_cnm_clinicvisits
						, mh_cnm_virtualvisits = excluded.mh_cnm_virtualvisits
						, mh_cnm_patients = excluded.mh_cnm_patients
						, sud_physicians_personnel = excluded.sud_physicians_personnel
						, sud_physicians_clinicvisits = excluded.sud_physicians_clinicvisits
						, sud_physicians_virtualvisits = excluded.sud_physicians_virtualvisits
						, sud_physicians_patients = excluded.sud_physicians_patients
						, sud_np_personnel = excluded.sud_np_personnel
						, sud_np_clinicvisits = excluded.sud_np_clinicvisits
						, sud_np_virtualvisits = excluded.sud_np_virtualvisits
						, sud_np_patients = excluded.sud_np_patients
						, sud_pa_personnel = excluded.sud_pa_personnel
						, sud_pa_clinicvisits = excluded.sud_pa_clinicvisits
						, sud_pa_virtualvisits = excluded.sud_pa_virtualvisits
						, sud_pa_patients = excluded.sud_pa_patients
						, sud_cnm_personnel = excluded.sud_cnm_personnel
						, sud_cnm_clinicvisits = excluded.sud_cnm_clinicvisits
						, sud_cnm_virtualvisits = excluded.sud_cnm_virtualvisits
						, sud_cnm_patients = excluded.sud_cnm_patients
						, sud_psychiatrists_personnel = excluded.sud_psychiatrists_personnel
						, sud_psychiatrists_clinicvisits = excluded.sud_psychiatrists_clinicvisits
						, sud_psychiatrists_virtualvisits = excluded.sud_psychiatrists_virtualvisits
						, sud_psychiatrists_patients = excluded.sud_psychiatrists_patients
						, sud_licpsychologists_personnel = excluded.sud_licpsychologists_personnel
						, sud_licpsychologists_clinicvisits = excluded.sud_licpsychologists_clinicvisits
						, sud_licpsychologists_virtualvisits = excluded.sud_licpsychologists_virtualvisits
						, sud_licpsychologists_patients = excluded.sud_licpsychologists_patients
						, sud_licsocialworkers_personnel = excluded.sud_licsocialworkers_personnel
						, sud_licsocialworkers_clinicvisits = excluded.sud_licsocialworkers_clinicvisits
						, sud_licsocialworkers_virtualvisits = excluded.sud_licsocialworkers_virtualvisits
						, sud_licsocialworkers_patients = excluded.sud_licsocialworkers_patients
						, sud_otherlicmhprov_personnel = excluded.sud_otherlicmhprov_personnel
						, sud_otherlicmhprov_clinicvisits = excluded.sud_otherlicmhprov_clinicvisits
						, sud_otherlicmhprov_virtualvisits = excluded.sud_otherlicmhprov_virtualvisits
						, sud_otherlicmhprov_patients = excluded.sud_otherlicmhprov_patients
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						bh_servicedetail.last_modified < excluded.last_modified
					RETURNING bh_servicedetail.clinicid
		)
						
				SELECT count(*)
				FROM insertbh_servicedetail
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: BH_SERVICEDETAIL'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	
	RAISE NOTICE '...DENTAL Table';	
		
		WITH insertdental AS (
			INSERT INTO final.dental (
				 year
				, clinicid
				, dentists_paidprovfte
				, dentists_volunteers
				, dentists_volhours
				, dentists_paidvisits
				, dentists_volvisits
				, dentists_virtualvisits
				, dentalhygienists_paidprovfte
				, dentalhygienists_volunteers
				, dentalhygienists_volhours
				, dentalhygienists_paidvisits
				, dentalhygienists_volvisits
				, dentalhygienists_virtualvisits
				, ecphygienists_paidprovfte
				, ecphygienists_volunteers
				, ecphygienists_volhours
				, ecphygienists_paidvisits
				, ecphygienists_volvisits
				, dentaltherapists_paidprovfte
				, dentaltherapists_volunteers
				, dentaltherapists_volhours
				, dentaltherapists_paidvisits
				, dentaltherapists_volvisits
				, dentaltherapists_virtualvisits
				, dentalassist_paidprovfte
				, dentalassist_volunteers
				, dentalassist_volhours
				, patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
 
			)
				  
				SELECT 
					 year
					, clinicid
					, dentists_paidprovfte
					, dentists_volunteers
					, dentists_volhours
					, dentists_paidvisits
					, dentists_volvisits
					, dentists_virtualvisits
					, dentalhygienists_paidprovfte
					, dentalhygienists_volunteers
					, dentalhygienists_volhours
					, dentalhygienists_paidvisits
					, dentalhygienists_volvisits
					, dentalhygienists_virtualvisits
					, ecphygienists_paidprovfte
					, ecphygienists_volunteers
					, ecphygienists_volhours
					, ecphygienists_paidvisits
					, ecphygienists_volvisits
					, dentaltherapists_paidprovfte
					, dentaltherapists_volunteers
					, dentaltherapists_volhours
					, dentaltherapists_paidvisits
					, dentaltherapists_volvisits
					, dentaltherapists_virtualvisits
					, dentalassist_paidprovfte
					, dentalassist_volunteers
					, dentalassist_volhours
					, patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.dental
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 dentists_paidprovfte = excluded.dentists_paidprovfte
						, dentists_volunteers = excluded.dentists_volunteers
						, dentists_volhours = excluded.dentists_volhours
						, dentists_paidvisits = excluded.dentists_paidvisits
						, dentists_volvisits = excluded.dentists_volvisits
						, dentists_virtualvisits = excluded.dentists_virtualvisits
						, dentalhygienists_paidprovfte = excluded.dentalhygienists_paidprovfte
						, dentalhygienists_volunteers = excluded.dentalhygienists_volunteers
						, dentalhygienists_volhours = excluded.dentalhygienists_volhours
						, dentalhygienists_paidvisits = excluded.dentalhygienists_paidvisits
						, dentalhygienists_volvisits = excluded.dentalhygienists_volvisits
						, dentalhygienists_virtualvisits = excluded.dentalhygienists_virtualvisits
						, ecphygienists_paidprovfte = excluded.ecphygienists_paidprovfte
						, ecphygienists_volunteers = excluded.ecphygienists_volunteers
						, ecphygienists_volhours = excluded.ecphygienists_volhours
						, ecphygienists_paidvisits = excluded.ecphygienists_paidvisits
						, ecphygienists_volvisits = excluded.ecphygienists_volvisits
						, dentaltherapists_paidprovfte = excluded.dentaltherapists_paidprovfte
						, dentaltherapists_volunteers = excluded.dentaltherapists_volunteers
						, dentaltherapists_volhours = excluded.dentaltherapists_volhours
						, dentaltherapists_paidvisits = excluded.dentaltherapists_paidvisits
						, dentaltherapists_volvisits = excluded.dentaltherapists_volvisits
						, dentaltherapists_virtualvisits = excluded.dentaltherapists_virtualvisits
						, dentalassist_paidprovfte = excluded.dentalassist_paidprovfte
						, dentalassist_volunteers = excluded.dentalassist_volunteers
						, dentalassist_volhours = excluded.dentalassist_volhours
						, patients = excluded.patients
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						dental.last_modified < excluded.last_modified
					RETURNING dental.clinicid
		)
						
				SELECT count(*)
				FROM insertdental
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: DENTAL'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	
	RAISE NOTICE '...ENABLINGSVCS Table';	
		
		WITH insertenablingsvcs AS (
			INSERT INTO final.enablingsvcs (
				 year
				, clinicid
				, casemanagers_paidprovfte
				, casemanagers_volunteers
				, casemanagers_volhours
				, casemanagers_paidvisits
				, casemanagers_volvisits
				, casemanagers_virtualvisits
				, eduspecialists_paidprovfte
				, eduspecialists_volunteers
				, eduspecialists_volhours
				, eduspecialists_paidvisits
				, eduspecialists_volvisits
				, eduspecialists_virtualvisits
				, outreachpersonnel_paidprovfte
				, outreachpersonnel_volunteers
				, outreachpersonnel_volhours
				, transpersonnel_paidprovfte
				, transpersonnel_volunteers
				, transpersonnel_volhours
				, eligassistpersonnel_paidprovfte
				, eligassistpersonnel_volunteers
				, eligassistpersonnel_volhours
				, interpretationpersonnel_paidprovfte
				, interpretationpersonnel_volunteers
				, interpretationpersonnel_volhours
				, commhealthpersonnel_paidprovfte
				, commhealthpersonnel_volunteers
				, commhealthpersonnel_volhours
				, otherenablingservices_paidprovfte
				, otherenablingservices_volunteers
				, otherenablingservices_volhours
				, patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom
			)
				  
				SELECT
					 year
					, clinicid
					, casemanagers_paidprovfte
					, casemanagers_volunteers
					, casemanagers_volhours
					, casemanagers_paidvisits
					, casemanagers_volvisits
					, casemanagers_virtualvisits
					, eduspecialists_paidprovfte
					, eduspecialists_volunteers
					, eduspecialists_volhours
					, eduspecialists_paidvisits
					, eduspecialists_volvisits
					, eduspecialists_virtualvisits
					, outreachpersonnel_paidprovfte
					, outreachpersonnel_volunteers
					, outreachpersonnel_volhours
					, transpersonnel_paidprovfte
					, transpersonnel_volunteers
					, transpersonnel_volhours
					, eligassistpersonnel_paidprovfte
					, eligassistpersonnel_volunteers
					, eligassistpersonnel_volhours
					, interpretationpersonnel_paidprovfte
					, interpretationpersonnel_volunteers
					, interpretationpersonnel_volhours
					, commhealthpersonnel_paidprovfte
					, commhealthpersonnel_volunteers
					, commhealthpersonnel_volhours
					, otherenablingservices_paidprovfte
					, otherenablingservices_volunteers
					, otherenablingservices_volhours
					, patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.enablingsvcs
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						 casemanagers_paidprovfte = excluded.casemanagers_paidprovfte
						, casemanagers_volunteers = excluded.casemanagers_volunteers
						, casemanagers_volhours = excluded.casemanagers_volhours
						, casemanagers_paidvisits = excluded.casemanagers_paidvisits
						, casemanagers_volvisits = excluded.casemanagers_volvisits
						, casemanagers_virtualvisits = excluded.casemanagers_virtualvisits
						, eduspecialists_paidprovfte = excluded.eduspecialists_paidprovfte
						, eduspecialists_volunteers = excluded.eduspecialists_volunteers
						, eduspecialists_volhours = excluded.eduspecialists_volhours
						, eduspecialists_paidvisits = excluded.eduspecialists_paidvisits
						, eduspecialists_volvisits = excluded.eduspecialists_volvisits
						, eduspecialists_virtualvisits = excluded.eduspecialists_virtualvisits
						, outreachpersonnel_paidprovfte = excluded.outreachpersonnel_paidprovfte
						, outreachpersonnel_volunteers = excluded.outreachpersonnel_volunteers
						, outreachpersonnel_volhours = excluded.outreachpersonnel_volhours
						, transpersonnel_paidprovfte = excluded.transpersonnel_paidprovfte
						, transpersonnel_volunteers = excluded.transpersonnel_volunteers
						, transpersonnel_volhours = excluded.transpersonnel_volhours
						, eligassistpersonnel_paidprovfte = excluded.eligassistpersonnel_paidprovfte
						, eligassistpersonnel_volunteers = excluded.eligassistpersonnel_volunteers
						, eligassistpersonnel_volhours = excluded.eligassistpersonnel_volhours
						, interpretationpersonnel_paidprovfte = excluded.interpretationpersonnel_paidprovfte
						, interpretationpersonnel_volunteers = excluded.interpretationpersonnel_volunteers
						, interpretationpersonnel_volhours = excluded.interpretationpersonnel_volhours
						, commhealthpersonnel_paidprovfte = excluded.commhealthpersonnel_paidprovfte
						, commhealthpersonnel_volunteers = excluded.commhealthpersonnel_volunteers
						, commhealthpersonnel_volhours = excluded.commhealthpersonnel_volhours
						, otherenablingservices_paidprovfte = excluded.otherenablingservices_paidprovfte
						, otherenablingservices_volunteers = excluded.otherenablingservices_volunteers
						, otherenablingservices_volhours = excluded.otherenablingservices_volhours
						, patients = excluded.patients
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						enablingsvcs.last_modified < excluded.last_modified
					RETURNING enablingsvcs.clinicid
		)
						
				SELECT count(*)
				FROM insertenablingsvcs
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: ENABLINGSVCS'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;



	RAISE NOTICE '...MEDICAL Table';	
		
		WITH insertmedical AS (
			INSERT INTO final.medical (
				 year
				, clinicid
				, familyphysicians_paidprovfte
				, familyphysicians_volunteers
				, familyphysicians_volhours
				, familyphysicians_paidvisits
				, familyphysicians_volvisits
				, familyphysicians_virtualvisits
				, generalpractitioners_paidprovfte
				, generalpractitioners_volunteers
				, generalpractitioners_volhours
				, generalpractitioners_paidvisits
				, generalpractitioners_volvisits
				, generalpractitioners_virtualvisits
				, internists_paidprovfte
				, internists_volunteers
				, internists_volhours
				, internists_paidvisits
				, internists_volvisits
				, internists_virtualvisits
				, obgyn_paidprovfte
				, obgyn_volunteers
				, obgyn_volhours
				, obgyn_paidvisits
				, obgyn_volvisits
				, obgyn_virtualvisits
				, pediatricians_paidprovfte
				, pediatricians_volunteers
				, pediatricians_volhours
				, pediatricians_paidvisits
				, pediatricians_volvisits
				, pediatricians_virtualvisits
				, otherspecphys_paidprovfte
				, otherspecphys_volunteers
				, otherspecphys_volhours
				, otherspecphys_paidvisits
				, otherspecphys_volvisits
				, otherspecphys_virtualvisits
				, np_paidprovfte
				, np_volunteers
				, np_volhours
				, np_paidvisits
				, np_volvisits
				, np_virtualvisits
				, pa_paidprovfte
				, pa_volunteers
				, pa_volhours
				, pa_paidvisits
				, pa_volvisits
				, pa_virtualvisits
				, cnm_paidprovfte
				, cnm_volunteers
				, cnm_volhours
				, cnm_paidvisits
				, cnm_volvisits
				, cnm_virtualvisits
				, nurses_paidprovfte
				, nurses_volunteers
				, nurses_volhours
				, nurses_paidvisits
				, nurses_volvisits
				, nurses_virtualvisits
				, kanbnurses_paidprovfte
				, kanbnurses_volunteers
				, kanbnurses_volhours
				, kanbnurses_paidvisits
				, kanbnurses_volvisits
				, othermedpersonnel_paidprovfte
				, othermedpersonnel_volunteers
				, othermedpersonnel_volhours
				, labpersonnel_paidprovfte
				, labpersonnel_volunteers
				, labpersonnel_volhours
				, xraypersonnel_paidprovfte
				, xraypersonnel_volunteers
				, xraypersonnel_volhours
				, patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT
					 year
					, clinicid
					, familyphysicians_paidprovfte
					, familyphysicians_volunteers
					, familyphysicians_volhours
					, familyphysicians_paidvisits
					, familyphysicians_volvisits
					, familyphysicians_virtualvisits
					, generalpractitioners_paidprovfte
					, generalpractitioners_volunteers
					, generalpractitioners_volhours
					, generalpractitioners_paidvisits
					, generalpractitioners_volvisits
					, generalpractitioners_virtualvisits
					, internists_paidprovfte
					, internists_volunteers
					, internists_volhours
					, internists_paidvisits
					, internists_volvisits
					, internists_virtualvisits
					, obgyn_paidprovfte
					, obgyn_volunteers
					, obgyn_volhours
					, obgyn_paidvisits
					, obgyn_volvisits
					, obgyn_virtualvisits
					, pediatricians_paidprovfte
					, pediatricians_volunteers
					, pediatricians_volhours
					, pediatricians_paidvisits
					, pediatricians_volvisits
					, pediatricians_virtualvisits
					, otherspecphys_paidprovfte
					, otherspecphys_volunteers
					, otherspecphys_volhours
					, otherspecphys_paidvisits
					, otherspecphys_volvisits
					, otherspecphys_virtualvisits
					, np_paidprovfte
					, np_volunteers
					, np_volhours
					, np_paidvisits
					, np_volvisits
					, np_virtualvisits
					, pa_paidprovfte
					, pa_volunteers
					, pa_volhours
					, pa_paidvisits
					, pa_volvisits
					, pa_virtualvisits
					, cnm_paidprovfte
					, cnm_volunteers
					, cnm_volhours
					, cnm_paidvisits
					, cnm_volvisits
					, cnm_virtualvisits
					, nurses_paidprovfte
					, nurses_volunteers
					, nurses_volhours
					, nurses_paidvisits
					, nurses_volvisits
					, nurses_virtualvisits
					, kanbnurses_paidprovfte
					, kanbnurses_volunteers
					, kanbnurses_volhours
					, kanbnurses_paidvisits
					, kanbnurses_volvisits
					, othermedpersonnel_paidprovfte
					, othermedpersonnel_volunteers
					, othermedpersonnel_volhours
					, labpersonnel_paidprovfte
					, labpersonnel_volunteers
					, labpersonnel_volhours
					, xraypersonnel_paidprovfte
					, xraypersonnel_volunteers
					, xraypersonnel_volhours
					, patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.medical
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						familyphysicians_paidprovfte = excluded.familyphysicians_paidprovfte
						, familyphysicians_volunteers = excluded.familyphysicians_volunteers
						, familyphysicians_volhours = excluded.familyphysicians_volhours
						, familyphysicians_paidvisits = excluded.familyphysicians_paidvisits
						, familyphysicians_volvisits = excluded.familyphysicians_volvisits
						, familyphysicians_virtualvisits = excluded.familyphysicians_virtualvisits
						, generalpractitioners_paidprovfte = excluded.generalpractitioners_paidprovfte
						, generalpractitioners_volunteers = excluded.generalpractitioners_volunteers
						, generalpractitioners_volhours = excluded.generalpractitioners_volhours
						, generalpractitioners_paidvisits = excluded.generalpractitioners_paidvisits
						, generalpractitioners_volvisits = excluded.generalpractitioners_volvisits
						, generalpractitioners_virtualvisits = excluded.generalpractitioners_virtualvisits
						, internists_paidprovfte = excluded.internists_paidprovfte
						, internists_volunteers = excluded.internists_volunteers
						, internists_volhours = excluded.internists_volhours
						, internists_paidvisits = excluded.internists_paidvisits
						, internists_volvisits = excluded.internists_volvisits
						, internists_virtualvisits = excluded.internists_virtualvisits
						, obgyn_paidprovfte = excluded.obgyn_paidprovfte
						, obgyn_volunteers = excluded.obgyn_volunteers
						, obgyn_volhours = excluded.obgyn_volhours
						, obgyn_paidvisits = excluded.obgyn_paidvisits
						, obgyn_volvisits = excluded.obgyn_volvisits
						, obgyn_virtualvisits = excluded.obgyn_virtualvisits
						, pediatricians_paidprovfte = excluded.pediatricians_paidprovfte
						, pediatricians_volunteers = excluded.pediatricians_volunteers
						, pediatricians_volhours = excluded.pediatricians_volhours
						, pediatricians_paidvisits = excluded.pediatricians_paidvisits
						, pediatricians_volvisits = excluded.pediatricians_volvisits
						, pediatricians_virtualvisits = excluded.pediatricians_virtualvisits
						, otherspecphys_paidprovfte = excluded.otherspecphys_paidprovfte
						, otherspecphys_volunteers = excluded.otherspecphys_volunteers
						, otherspecphys_volhours = excluded.otherspecphys_volhours
						, otherspecphys_paidvisits = excluded.otherspecphys_paidvisits
						, otherspecphys_volvisits = excluded.otherspecphys_volvisits
						, otherspecphys_virtualvisits = excluded.otherspecphys_virtualvisits
						, np_paidprovfte = excluded.np_paidprovfte
						, np_volunteers = excluded.np_volunteers
						, np_volhours = excluded.np_volhours
						, np_paidvisits = excluded.np_paidvisits
						, np_volvisits = excluded.np_volvisits
						, np_virtualvisits = excluded.np_virtualvisits
						, pa_paidprovfte = excluded.pa_paidprovfte
						, pa_volunteers = excluded.pa_volunteers
						, pa_volhours = excluded.pa_volhours
						, pa_paidvisits = excluded.pa_paidvisits
						, pa_volvisits = excluded.pa_volvisits
						, pa_virtualvisits = excluded.pa_virtualvisits
						, cnm_paidprovfte = excluded.cnm_paidprovfte
						, cnm_volunteers = excluded.cnm_volunteers
						, cnm_volhours = excluded.cnm_volhours
						, cnm_paidvisits = excluded.cnm_paidvisits
						, cnm_volvisits = excluded.cnm_volvisits
						, cnm_virtualvisits = excluded.cnm_virtualvisits
						, nurses_paidprovfte = excluded.nurses_paidprovfte
						, nurses_volunteers = excluded.nurses_volunteers
						, nurses_volhours = excluded.nurses_volhours
						, nurses_paidvisits = excluded.nurses_paidvisits
						, nurses_volvisits = excluded.nurses_volvisits
						, nurses_virtualvisits = excluded.nurses_virtualvisits
						, kanbnurses_paidprovfte = excluded.kanbnurses_paidprovfte
						, kanbnurses_volunteers = excluded.kanbnurses_volunteers
						, kanbnurses_volhours = excluded.kanbnurses_volhours
						, kanbnurses_paidvisits = excluded.kanbnurses_paidvisits
						, kanbnurses_volvisits = excluded.kanbnurses_volvisits
						, othermedpersonnel_paidprovfte = excluded.othermedpersonnel_paidprovfte
						, othermedpersonnel_volunteers = excluded.othermedpersonnel_volunteers
						, othermedpersonnel_volhours = excluded.othermedpersonnel_volhours
						, labpersonnel_paidprovfte = excluded.labpersonnel_paidprovfte
						, labpersonnel_volunteers = excluded.labpersonnel_volunteers
						, labpersonnel_volhours = excluded.labpersonnel_volhours
						, xraypersonnel_paidprovfte = excluded.xraypersonnel_paidprovfte
						, xraypersonnel_volunteers = excluded.xraypersonnel_volunteers
						, xraypersonnel_volhours = excluded.xraypersonnel_volhours
						, patients = excluded.patients
						, data_extension = excluded.data_extension
						, status = excluded.status
						, created = excluded.created
						, created_bywhom = excluded.created_bywhom
						, last_modified = excluded.last_modified
						, last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						medical.last_modified < excluded.last_modified
					RETURNING medical.clinicid
		)
						
				SELECT count(*)
				FROM insertmedical
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: MEDICAL'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...OTHERPROFSVCS Table';	
		
		WITH insertotherprofsvcs AS (
			INSERT INTO final.otherprofsvcs (
				 year
				, clinicid
				, otherprofsvcs_paidprovfte
				, otherprofsvcs_volunteers
				, otherprofsvcs_volhours
				, otherprofsvcs_paidvisits
				, otherprofsvcs_volvisits
				, otherprofsvcs_virtualvisits
				, patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom
			)
				  
				SELECT
					 year
					, clinicid
					, otherprofsvcs_paidprovfte
					, otherprofsvcs_volunteers
					, otherprofsvcs_volhours
					, otherprofsvcs_paidvisits
					, otherprofsvcs_volvisits
					, otherprofsvcs_virtualvisits
					, patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.otherprofsvcs
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  otherprofsvcs_paidprovfte = excluded.otherprofsvcs_paidprovfte
						 , otherprofsvcs_volunteers = excluded.otherprofsvcs_volunteers
						 , otherprofsvcs_volhours = excluded.otherprofsvcs_volhours
						 , otherprofsvcs_paidvisits = excluded.otherprofsvcs_paidvisits
						 , otherprofsvcs_volvisits = excluded.otherprofsvcs_volvisits
						 , otherprofsvcs_virtualvisits = excluded.otherprofsvcs_virtualvisits
						 , patients = excluded.patients
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						otherprofsvcs.last_modified < excluded.last_modified
					RETURNING otherprofsvcs.clinicid
		)
						
				SELECT count(*)
				FROM insertotherprofsvcs
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: OTHERPROFSVCS'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...OTHERPROGRAMSSERVICES Table';	
		
		WITH insertotherprogramsservices AS (
			INSERT INTO final.otherprogramsservices (
				 year
				, clinicid
				, qipersonnel_paidprovfte
				, qipersonnel_volunteers
				, qipersonnel_volhours
				, ops_paidprovfte
				, ops_volunteers
				, ops_volhours
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					 year
					, clinicid
					, qipersonnel_paidprovfte
					, qipersonnel_volunteers
					, qipersonnel_volhours
					, ops_paidprovfte
					, ops_volunteers
					, ops_volhours
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.otherprogramsservices
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  qipersonnel_paidprovfte = excluded.qipersonnel_paidprovfte
						 , qipersonnel_volunteers = excluded.qipersonnel_volunteers
						 , qipersonnel_volhours = excluded.qipersonnel_volhours
						 , ops_paidprovfte = excluded.ops_paidprovfte
						 , ops_volunteers = excluded.ops_volunteers
						 , ops_volhours = excluded.ops_volhours
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						otherprogramsservices.last_modified < excluded.last_modified
					RETURNING otherprogramsservices.clinicid
		)
						
				SELECT count(*)
				FROM insertotherprogramsservices
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: OTHERPROGRAMSSERVICES'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...PHARMACY Table';	
		
		WITH insertpharmacy AS (
			INSERT INTO final.pharmacy (
				 year
				, clinicid
				, pharmacypersonnel_paidprovfte
				, pharmacypersonnel_volunteers
				, pharmacypersonnel_volhours
				, papprogram_count
				, _340bprogram_count
				, unusedmedprogram_count
				, papprogram_patients
				, _340bprogram_patients
				, unusedmedprogram_patients
				, _340bclinic
				, _340bclinic_type
				, _340b_clinicowned
				, _340b_clinicmanaged
				, _340b_external
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT 
					year
					, clinicid
					, pharmacypersonnel_paidprovfte
					, pharmacypersonnel_volunteers
					, pharmacypersonnel_volhours
					, papprogram_count
					, _340bprogram_count
					, unusedmedprogram_count
					, papprogram_patients
					, _340bprogram_patients
					, unusedmedprogram_patients
					, _340bclinic
					, _340bclinic_type
					, _340b_clinicowned
					, _340b_clinicmanaged
					, _340b_external
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom  
				FROM staging.pharmacy
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  pharmacypersonnel_paidprovfte = excluded.pharmacypersonnel_paidprovfte
						 , pharmacypersonnel_volunteers = excluded.pharmacypersonnel_volunteers
						 , pharmacypersonnel_volhours = excluded.pharmacypersonnel_volhours
						 , papprogram_count = excluded.papprogram_count
						 , _340bprogram_count = excluded._340bprogram_count
						 , unusedmedprogram_count = excluded.unusedmedprogram_count
						 , papprogram_patients = excluded.papprogram_patients
						 , _340bprogram_patients = excluded._340bprogram_patients
						 , unusedmedprogram_patients = excluded.unusedmedprogram_patients
						 , _340bclinic = excluded._340bclinic
						 , _340bclinic_type = excluded._340bclinic_type
						 , _340b_clinicowned = excluded._340b_clinicowned
						 , _340b_clinicmanaged = excluded._340b_clinicmanaged
						 , _340b_external = excluded._340b_external
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						pharmacy.last_modified < excluded.last_modified
					RETURNING pharmacy.clinicid
		)
						
				SELECT count(*)
				FROM insertpharmacy
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: PHARMACY'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

RAISE NOTICE '...SCHOOLSERVICESDATA Table';	
		
		WITH insertschoolservicesdata AS (
			INSERT INTO final.schoolservicesdata (
				 year
				, clinicid
				, q1
				, q2
				, q3_pm
				, q3_bh
				, q3_oral
				, q3_sealants
				, q3_dental
				, q3_sud
				, q3_int
				, q3_vision
				, q3_rx
				, q3_other
				, q3_othertext
				, q1_sbhc
				, q2_sbhc
				, q3_sbhc_sn
				, q3_sbhc_counsel
				, q3_sbhc_dental
				, q3_sbhc_pm
				, q3_sbhc_oral
				, q3_sbhc_sealants
				, q3_sbhc_sud
				, q3_sbhc_int
				, q3_sbhc_vision
				, q3_sbhc_rx
				, q3_sbhc_other
				, q3_sbhc_othertext
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					 year
					, clinicid
					, q1
					, q2
					, q3_pm
					, q3_bh
					, q3_oral
					, q3_sealants
					, q3_dental
					, q3_sud
					, q3_int
					, q3_vision
					, q3_rx
					, q3_other
					, q3_othertext
					, q1_sbhc
					, q2_sbhc
					, q3_sbhc_sn
					, q3_sbhc_counsel
					, q3_sbhc_dental
					, q3_sbhc_pm
					, q3_sbhc_oral
					, q3_sbhc_sealants
					, q3_sbhc_sud
					, q3_sbhc_int
					, q3_sbhc_vision
					, q3_sbhc_rx
					, q3_sbhc_other
					, q3_sbhc_othertext
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.schoolservicesdata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  q1 = excluded.q1
						 , q2 = excluded.q2
						 , q3_pm = excluded.q3_pm
						 , q3_bh = excluded.q3_bh
						 , q3_oral = excluded.q3_oral
						 , q3_sealants = excluded.q3_sealants
						 , q3_dental = excluded.q3_dental
						 , q3_sud = excluded.q3_sud
						 , q3_int = excluded.q3_int
						 , q3_vision = excluded.q3_vision
						 , q3_rx = excluded.q3_rx
						 , q3_other = excluded.q3_other
						 , q3_othertext = excluded.q3_othertext
						 , q1_sbhc = excluded.q1_sbhc
						 , q2_sbhc = excluded.q2_sbhc
						 , q3_sbhc_sn = excluded.q3_sbhc_sn
						 , q3_sbhc_counsel = excluded.q3_sbhc_counsel
						 , q3_sbhc_dental = excluded.q3_sbhc_dental
						 , q3_sbhc_pm = excluded.q3_sbhc_pm
						 , q3_sbhc_oral = excluded.q3_sbhc_oral
						 , q3_sbhc_sealants = excluded.q3_sbhc_sealants
						 , q3_sbhc_sud = excluded.q3_sbhc_sud
						 , q3_sbhc_int = excluded.q3_sbhc_int
						 , q3_sbhc_vision = excluded.q3_sbhc_vision
						 , q3_sbhc_rx = excluded.q3_sbhc_rx
						 , q3_sbhc_other = excluded.q3_sbhc_other
						 , q3_sbhc_othertext = excluded.q3_sbhc_othertext
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						schoolservicesdata.last_modified < excluded.last_modified
					RETURNING schoolservicesdata.clinicid
		)
						
				SELECT count(*)
				FROM insertschoolservicesdata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: SCHOOLSERVICESDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...VISION Table';	
		
		WITH insertvision AS (
			INSERT INTO final.vision (
				 year
				, clinicid
				, ophthalmologists_paidprovfte
				, ophthalmologists_volunteers
				, ophthalmologists_volhours
				, ophthalmologists_paidvisits
				, ophthalmologists_volvisits
				, ophthalmologists_virtualvisits
				, optometrists_paidprovfte
				, optometrists_volunteers
				, optometrists_volhours
				, optometrists_paidvisits
				, optometrists_volvisits
				, optometrists_virtualvisits
				, othervisionpersonnel_paidprovfte
				, othervisionpersonnel_volunteers
				, othervisionpersonnel_volhours
				, patients
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					 year
					, clinicid
					, ophthalmologists_paidprovfte
					, ophthalmologists_volunteers
					, ophthalmologists_volhours
					, ophthalmologists_paidvisits
					, ophthalmologists_volvisits
					, ophthalmologists_virtualvisits
					, optometrists_paidprovfte
					, optometrists_volunteers
					, optometrists_volhours
					, optometrists_paidvisits
					, optometrists_volvisits
					, optometrists_virtualvisits
					, othervisionpersonnel_paidprovfte
					, othervisionpersonnel_volunteers
					, othervisionpersonnel_volhours
					, patients
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom  
				FROM staging.vision
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  ophthalmologists_paidprovfte = excluded.ophthalmologists_paidprovfte
						 , ophthalmologists_volunteers = excluded.ophthalmologists_volunteers
						 , ophthalmologists_volhours = excluded.ophthalmologists_volhours
						 , ophthalmologists_paidvisits = excluded.ophthalmologists_paidvisits
						 , ophthalmologists_volvisits = excluded.ophthalmologists_volvisits
						 , ophthalmologists_virtualvisits = excluded.ophthalmologists_virtualvisits
						 , optometrists_paidprovfte = excluded.optometrists_paidprovfte
						 , optometrists_volunteers = excluded.optometrists_volunteers
						 , optometrists_volhours = excluded.optometrists_volhours
						 , optometrists_paidvisits = excluded.optometrists_paidvisits
						 , optometrists_volvisits = excluded.optometrists_volvisits
						 , optometrists_virtualvisits = excluded.optometrists_virtualvisits
						 , othervisionpersonnel_paidprovfte = excluded.othervisionpersonnel_paidprovfte
						 , othervisionpersonnel_volunteers = excluded.othervisionpersonnel_volunteers
						 , othervisionpersonnel_volhours = excluded.othervisionpersonnel_volhours
						 , patients = excluded.patients
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						vision.last_modified < excluded.last_modified
					RETURNING vision.clinicid
		)
						
				SELECT count(*)
				FROM insertvision
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: VISION'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...WORKFORCEDATA Table';	
		
		WITH insertworkforcedata AS (
			INSERT INTO final.workforcedata (
				 year
				, clinicid
				, q1
				, q2
				, q3
				, q4
				, q5
				, q6
				, q6_othertext
				, q7
				, q7_othertext
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT 
					year
					, clinicid
					, q1
					, q2
					, q3
					, q4
					, q5
					, q6
					, q6_othertext
					, q7
					, q7_othertext
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom    
				FROM staging.workforcedata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  q1 = excluded.q1
						 , q2 = excluded.q2
						 , q3 = excluded.q3
						 , q4 = excluded.q4
						 , q5 = excluded.q5
						 , q6 = excluded.q6
						 , q6_othertext = excluded.q6_othertext
						 , q7 = excluded.q7
						 , q7_othertext = excluded.q7_othertext
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						workforcedata.last_modified < excluded.last_modified
					RETURNING workforcedata.clinicid
		)
						
				SELECT count(*)
				FROM insertworkforcedata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: WORKFORCEDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...WORKFORCETRAINING Table';	
		
		WITH insertworkforcetraining AS (
			INSERT INTO final.workforcetraining (
				 year
				, clinicid
				, familyphysicians_pregrad
				, familyphysicians_postgrad
				, generalpractitioners_pregrad
				, generalpractitioners_postgrad
				, internists_pregrad
				, internists_postgrad
				, obgyn_pregrad
				, obgyn_postgrad
				, pediatricians_pregrad
				, pediatricians_postgrad
				, otherspecphys_pregrad
				, otherspecphys_postgrad
				, ma_pregrad
				, ma_postgrad
				, pa_pregrad
				, pa_postgrad
				, np_pregrad
				, np_postgrad
				, cnm_pregrad
				, cnm_postgrad
				, rn_pregrad
				, rn_postgrad
				, licnurses_pregrad
				, licnurses_postgrad
				, dentists_pregrad
				, dentists_postgrad
				, dentaltherapists_pregrad
				, dentaltherapists_postgrad
				, dentalhygienists_pregrad
				, dentalhygienists_postgrad
				, ophthalmologists_pregrad
				, ophthalmologists_postgrad
				, optometrists_pregrad
				, optometrists_postgrad
				, psychiatrists_pregrad
				, psychiatrists_postgrad
				, psychologists_pregrad
				, psychologists_postgrad
				, socialworkers_pregrad
				, socialworkers_postgrad
				, profcounselors_pregrad
				, profcounselors_postgrad
				, mftherapists_pregrad
				, mftherapists_postgrad
				, psynursespec_pregrad
				, psynursespec_postgrad
				, mhnp_pregrad
				, mhnp_postgrad
				, mhpa_pregrad
				, mhpa_postgrad
				, sudp_pregrad
				, sudp_postgrad
				, chiropractors_pregrad
				, chiropractors_postgrad
				, dietitians_pregrad
				, dietitians_postgrad
				, pharmacists_pregrad
				, pharmacists_postgrad
				, other_pregrad
				, other_postgrad
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT 
					 year
					, clinicid
					, familyphysicians_pregrad
					, familyphysicians_postgrad
					, generalpractitioners_pregrad
					, generalpractitioners_postgrad
					, internists_pregrad
					, internists_postgrad
					, obgyn_pregrad
					, obgyn_postgrad
					, pediatricians_pregrad
					, pediatricians_postgrad
					, otherspecphys_pregrad
					, otherspecphys_postgrad
					, ma_pregrad
					, ma_postgrad
					, pa_pregrad
					, pa_postgrad
					, np_pregrad
					, np_postgrad
					, cnm_pregrad
					, cnm_postgrad
					, rn_pregrad
					, rn_postgrad
					, licnurses_pregrad
					, licnurses_postgrad
					, dentists_pregrad
					, dentists_postgrad
					, dentaltherapists_pregrad
					, dentaltherapists_postgrad
					, dentalhygienists_pregrad
					, dentalhygienists_postgrad
					, ophthalmologists_pregrad
					, ophthalmologists_postgrad
					, optometrists_pregrad
					, optometrists_postgrad
					, psychiatrists_pregrad
					, psychiatrists_postgrad
					, psychologists_pregrad
					, psychologists_postgrad
					, socialworkers_pregrad
					, socialworkers_postgrad
					, profcounselors_pregrad
					, profcounselors_postgrad
					, mftherapists_pregrad
					, mftherapists_postgrad
					, psynursespec_pregrad
					, psynursespec_postgrad
					, mhnp_pregrad
					, mhnp_postgrad
					, mhpa_pregrad
					, mhpa_postgrad
					, sudp_pregrad
					, sudp_postgrad
					, chiropractors_pregrad
					, chiropractors_postgrad
					, dietitians_pregrad
					, dietitians_postgrad
					, pharmacists_pregrad
					, pharmacists_postgrad
					, other_pregrad
					, other_postgrad
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom  
				FROM staging.workforcetraining
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  familyphysicians_pregrad = excluded.familyphysicians_pregrad
						 , familyphysicians_postgrad = excluded.familyphysicians_postgrad
						 , generalpractitioners_pregrad = excluded.generalpractitioners_pregrad
						 , generalpractitioners_postgrad = excluded.generalpractitioners_postgrad
						 , internists_pregrad = excluded.internists_pregrad
						 , internists_postgrad = excluded.internists_postgrad
						 , obgyn_pregrad = excluded.obgyn_pregrad
						 , obgyn_postgrad = excluded.obgyn_postgrad
						 , pediatricians_pregrad = excluded.pediatricians_pregrad
						 , pediatricians_postgrad = excluded.pediatricians_postgrad
						 , otherspecphys_pregrad = excluded.otherspecphys_pregrad
						 , otherspecphys_postgrad = excluded.otherspecphys_postgrad
						 , ma_pregrad = excluded.ma_pregrad
						 , ma_postgrad = excluded.ma_postgrad
						 , pa_pregrad = excluded.pa_pregrad
						 , pa_postgrad = excluded.pa_postgrad
						 , np_pregrad = excluded.np_pregrad
						 , np_postgrad = excluded.np_postgrad
						 , cnm_pregrad = excluded.cnm_pregrad
						 , cnm_postgrad = excluded.cnm_postgrad
						 , rn_pregrad = excluded.rn_pregrad
						 , rn_postgrad = excluded.rn_postgrad
						 , licnurses_pregrad = excluded.licnurses_pregrad
						 , licnurses_postgrad = excluded.licnurses_postgrad
						 , dentists_pregrad = excluded.dentists_pregrad
						 , dentists_postgrad = excluded.dentists_postgrad
						 , dentaltherapists_pregrad = excluded.dentaltherapists_pregrad
						 , dentaltherapists_postgrad = excluded.dentaltherapists_postgrad
						 , dentalhygienists_pregrad = excluded.dentalhygienists_pregrad
						 , dentalhygienists_postgrad = excluded.dentalhygienists_postgrad
						 , ophthalmologists_pregrad = excluded.ophthalmologists_pregrad
						 , ophthalmologists_postgrad = excluded.ophthalmologists_postgrad
						 , optometrists_pregrad = excluded.optometrists_pregrad
						 , optometrists_postgrad = excluded.optometrists_postgrad
						 , psychiatrists_pregrad = excluded.psychiatrists_pregrad
						 , psychiatrists_postgrad = excluded.psychiatrists_postgrad
						 , psychologists_pregrad = excluded.psychologists_pregrad
						 , psychologists_postgrad = excluded.psychologists_postgrad
						 , socialworkers_pregrad = excluded.socialworkers_pregrad
						 , socialworkers_postgrad = excluded.socialworkers_postgrad
						 , profcounselors_pregrad = excluded.profcounselors_pregrad
						 , profcounselors_postgrad = excluded.profcounselors_postgrad
						 , mftherapists_pregrad = excluded.mftherapists_pregrad
						 , mftherapists_postgrad = excluded.mftherapists_postgrad
						 , psynursespec_pregrad = excluded.psynursespec_pregrad
						 , psynursespec_postgrad = excluded.psynursespec_postgrad
						 , mhnp_pregrad = excluded.mhnp_pregrad
						 , mhnp_postgrad = excluded.mhnp_postgrad
						 , mhpa_pregrad = excluded.mhpa_pregrad
						 , mhpa_postgrad = excluded.mhpa_postgrad
						 , sudp_pregrad = excluded.sudp_pregrad
						 , sudp_postgrad = excluded.sudp_postgrad
						 , chiropractors_pregrad = excluded.chiropractors_pregrad
						 , chiropractors_postgrad = excluded.chiropractors_postgrad
						 , dietitians_pregrad = excluded.dietitians_pregrad
						 , dietitians_postgrad = excluded.dietitians_postgrad
						 , pharmacists_pregrad = excluded.pharmacists_pregrad
						 , pharmacists_postgrad = excluded.pharmacists_postgrad
						 , other_pregrad = excluded.other_pregrad
						 , other_postgrad = excluded.other_postgrad
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						workforcetraining.last_modified < excluded.last_modified --Q?which update will represent new data?  or will we use last_modified?
					RETURNING workforcetraining.clinicid
		)
						
				SELECT count(*)
				FROM insertworkforcetraining
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: WORKFORCETRAINING'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE 'Processing Subject Area: Measures...';
	RAISE NOTICE '...CLINICALMEASURES Table';	
		
		WITH insertclinicalmeasures AS (
			INSERT INTO final.clinicalmeasures (
				 year
				, clinicid
				, diabeticpoorcontrol_numerator
				, diabeticpoorcontrol_denominator
				, diabeticpoorcontrol_nanr
				, diabeticnotesting_numerator
				, diabeticnotesting_denominator
				, diabeticnotesting_nanr
				, diabeticpoorcntrlnotest_numerator
				, diabeticpoorcntrlnotest_denominator
				, diabeticpoorcntrlnotest_nanr
				, hypertensive_numerator
				, hypertensive_denominator
				, hypertensive_nanr
				, tobaccouse_numerator
				, tobaccouse_denominator
				, tobaccouse_nanr
				, tobaccocessation_numerator
				, tobaccocessation_denominator
				, tobaccocessation_nanr
				, tobaccousecessation_numerator
				, tobaccousecessation_denominator
				, tobaccousecessation_nanr
				, adultweight_numerator
				, adultweight_denominator
				, adultweight_nanr
				, childweight_numerator
				, childweight_denominator
				, childweight_nanr
				, childimmunizations_numerator
				, childimmunizations_denominator
				, childimmunizations_nanr
				, cervicalcancer_numerator
				, cervicalcancer_denominator
				, cervicalcancer_nanr
				, asthmapharma_numerator
				, asthmapharma_denominator
				, asthmapharma_nanr
				, depression_numerator
				, depression_denominator
				, depression_nanr
				, depressionremission_numerator
				, depressionremission_denominator
				, depressionremission_nanr
				, ivd_numerator
				, ivd_denominator
				, ivd_nanr
				, colorectalcancer_numerator
				, colorectalcancer_denominator
				, colorectalcancer_nanr
				, breastcancer_numerator
				, breastcancer_denominator
				, breastcancer_nanr
				, coronaryarterydisease_numerator
				, coronaryarterydisease_denominator
				, coronaryarterydisease_nanr
				, hiv_numerator
				, hiv_denominator
				, hiv_nanr
				, hivscreening_numerator
				, hivscreening_denominator
				, hivscreening_nanr
				, ivd_ldlcntrl_numerator
				, ivd_ldlcntrl_denominator
				, ivd_ldlcntrl_nanr
				, fluoride_numerator
				, fluoride_denominator
				, fluoride_nanr
				, totalvisits
				, sealants6to9_numerator
				, sealants6to9_denominator
				, sealants6to9_nanr
				, treatmentplan_numerator
				, treatmentplan_denominator
				, treatmentplan_nanr
				, cariesrecall_numerator
				, cariesrecall_denominator
				, cariesrecall_nanr
				, riskassess_numerator
				, riskassess_denominator
				, riskassess_nanr
				, oraleval_numerator
				, oraleval_denominator
				, oraleval_nanr
				, topicalfluoride_numerator
				, topicalfluoride_denominator
				, topicalfluoride_nanr
				, sealants10to14_numerator
				, sealants10to14_denominator
				, sealants10to14_nanr
				, goalsetting_numerator
				, goalsetting_denominator
				, goalsetting_nanr
				, goalreview_numerator
				, goalreview_denominator
				, goalreview_nanr
				, recommendations_numerator
				, recommendations_denominator
				, recommendations_nanr
				, recall
				, recall_nanr
				, emergency
				, emergency_nanr
				, comprehensive
				, comprehensive_nanr
				, other
				, other_nanr
				, emergencyservices
				, emergencyservices_nanr
				, oralexams
				, oralexams_nanr
				, prophylaxis
				, prophylaxis_nanr
				, sealants
				, sealants_nanr
				, fluoridetreatment
				, fluoridetreatment_nanr
				, restorativeservices
				, restorativeservices_nanr
				, oralsurgery
				, oralsurgery_nanr
				, rehabservices
				, rehabservices_nanr
				, medassistprov_numerator
				, medassistprov_denominator
				, medassistprov_nanr
				, sudmat_numerator
				, sudmat_denominator
				, sudmat_nanr
				, sbirt_numerator
				, sbirt_denominator
				, sbirt_nanr
				, adolescentsud_numerator
				, adolescentsud_denominator
				, adolescentsud_nanr
				, notes
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					year
					, clinicid
					, diabeticpoorcontrol_numerator
					, diabeticpoorcontrol_denominator
					, diabeticpoorcontrol_nanr
					, diabeticnotesting_numerator
					, diabeticnotesting_denominator
					, diabeticnotesting_nanr
					, diabeticpoorcntrlnotest_numerator
					, diabeticpoorcntrlnotest_denominator
					, diabeticpoorcntrlnotest_nanr
					, hypertensive_numerator
					, hypertensive_denominator
					, hypertensive_nanr
					, tobaccouse_numerator
					, tobaccouse_denominator
					, tobaccouse_nanr
					, tobaccocessation_numerator
					, tobaccocessation_denominator
					, tobaccocessation_nanr
					, tobaccousecessation_numerator
					, tobaccousecessation_denominator
					, tobaccousecessation_nanr
					, adultweight_numerator
					, adultweight_denominator
					, adultweight_nanr
					, childweight_numerator
					, childweight_denominator
					, childweight_nanr
					, childimmunizations_numerator
					, childimmunizations_denominator
					, childimmunizations_nanr
					, cervicalcancer_numerator
					, cervicalcancer_denominator
					, cervicalcancer_nanr
					, asthmapharma_numerator
					, asthmapharma_denominator
					, asthmapharma_nanr
					, depression_numerator
					, depression_denominator
					, depression_nanr
					, depressionremission_numerator
					, depressionremission_denominator
					, depressionremission_nanr
					, ivd_numerator
					, ivd_denominator
					, ivd_nanr
					, colorectalcancer_numerator
					, colorectalcancer_denominator
					, colorectalcancer_nanr
					, breastcancer_numerator
					, breastcancer_denominator
					, breastcancer_nanr
					, coronaryarterydisease_numerator
					, coronaryarterydisease_denominator
					, coronaryarterydisease_nanr
					, hiv_numerator
					, hiv_denominator
					, hiv_nanr
					, hivscreening_numerator
					, hivscreening_denominator
					, hivscreening_nanr
					, ivd_ldlcntrl_numerator
					, ivd_ldlcntrl_denominator
					, ivd_ldlcntrl_nanr
					, fluoride_numerator
					, fluoride_denominator
					, fluoride_nanr
					, totalvisits
					, sealants6to9_numerator
					, sealants6to9_denominator
					, sealants6to9_nanr
					, treatmentplan_numerator
					, treatmentplan_denominator
					, treatmentplan_nanr
					, cariesrecall_numerator
					, cariesrecall_denominator
					, cariesrecall_nanr
					, riskassess_numerator
					, riskassess_denominator
					, riskassess_nanr
					, oraleval_numerator
					, oraleval_denominator
					, oraleval_nanr
					, topicalfluoride_numerator
					, topicalfluoride_denominator
					, topicalfluoride_nanr
					, sealants10to14_numerator
					, sealants10to14_denominator
					, sealants10to14_nanr
					, goalsetting_numerator
					, goalsetting_denominator
					, goalsetting_nanr
					, goalreview_numerator
					, goalreview_denominator
					, goalreview_nanr
					, recommendations_numerator
					, recommendations_denominator
					, recommendations_nanr
					, recall
					, recall_nanr
					, emergency
					, emergency_nanr
					, comprehensive
					, comprehensive_nanr
					, other
					, other_nanr
					, emergencyservices
					, emergencyservices_nanr
					, oralexams
					, oralexams_nanr
					, prophylaxis
					, prophylaxis_nanr
					, sealants
					, sealants_nanr
					, fluoridetreatment
					, fluoridetreatment_nanr
					, restorativeservices
					, restorativeservices_nanr
					, oralsurgery
					, oralsurgery_nanr
					, rehabservices
					, rehabservices_nanr
					, medassistprov_numerator
					, medassistprov_denominator
					, medassistprov_nanr
					, sudmat_numerator
					, sudmat_denominator
					, sudmat_nanr
					, sbirt_numerator
					, sbirt_denominator
					, sbirt_nanr
					, adolescentsud_numerator
					, adolescentsud_denominator
					, adolescentsud_nanr
					, notes
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.clinicalmeasures
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  diabeticpoorcontrol_numerator = excluded.diabeticpoorcontrol_numerator
						 , diabeticpoorcontrol_denominator = excluded.diabeticpoorcontrol_denominator
						 , diabeticpoorcontrol_nanr = excluded.diabeticpoorcontrol_nanr
						 , diabeticnotesting_numerator = excluded.diabeticnotesting_numerator
						 , diabeticnotesting_denominator = excluded.diabeticnotesting_denominator
						 , diabeticnotesting_nanr = excluded.diabeticnotesting_nanr
						 , diabeticpoorcntrlnotest_numerator = excluded.diabeticpoorcntrlnotest_numerator
						 , diabeticpoorcntrlnotest_denominator = excluded.diabeticpoorcntrlnotest_denominator
						 , diabeticpoorcntrlnotest_nanr = excluded.diabeticpoorcntrlnotest_nanr
						 , hypertensive_numerator = excluded.hypertensive_numerator
						 , hypertensive_denominator = excluded.hypertensive_denominator
						 , hypertensive_nanr = excluded.hypertensive_nanr
						 , tobaccouse_numerator = excluded.tobaccouse_numerator
						 , tobaccouse_denominator = excluded.tobaccouse_denominator
						 , tobaccouse_nanr = excluded.tobaccouse_nanr
						 , tobaccocessation_numerator = excluded.tobaccocessation_numerator
						 , tobaccocessation_denominator = excluded.tobaccocessation_denominator
						 , tobaccocessation_nanr = excluded.tobaccocessation_nanr
						 , tobaccousecessation_numerator = excluded.tobaccousecessation_numerator
						 , tobaccousecessation_denominator = excluded.tobaccousecessation_denominator
						 , tobaccousecessation_nanr = excluded.tobaccousecessation_nanr
						 , adultweight_numerator = excluded.adultweight_numerator
						 , adultweight_denominator = excluded.adultweight_denominator
						 , adultweight_nanr = excluded.adultweight_nanr
						 , childweight_numerator = excluded.childweight_numerator
						 , childweight_denominator = excluded.childweight_denominator
						 , childweight_nanr = excluded.childweight_nanr
						 , childimmunizations_numerator = excluded.childimmunizations_numerator
						 , childimmunizations_denominator = excluded.childimmunizations_denominator
						 , childimmunizations_nanr = excluded.childimmunizations_nanr
						 , cervicalcancer_numerator = excluded.cervicalcancer_numerator
						 , cervicalcancer_denominator = excluded.cervicalcancer_denominator
						 , cervicalcancer_nanr = excluded.cervicalcancer_nanr
						 , asthmapharma_numerator = excluded.asthmapharma_numerator
						 , asthmapharma_denominator = excluded.asthmapharma_denominator
						 , asthmapharma_nanr = excluded.asthmapharma_nanr
						 , depression_numerator = excluded.depression_numerator
						 , depression_denominator = excluded.depression_denominator
						 , depression_nanr = excluded.depression_nanr
						 , depressionremission_numerator = excluded.depressionremission_numerator
						 , depressionremission_denominator = excluded.depressionremission_denominator
						 , depressionremission_nanr = excluded.depressionremission_nanr
						 , ivd_numerator = excluded.ivd_numerator
						 , ivd_denominator = excluded.ivd_denominator
						 , ivd_nanr = excluded.ivd_nanr
						 , colorectalcancer_numerator = excluded.colorectalcancer_numerator
						 , colorectalcancer_denominator = excluded.colorectalcancer_denominator
						 , colorectalcancer_nanr = excluded.colorectalcancer_nanr
						 , breastcancer_numerator = excluded.breastcancer_numerator
						 , breastcancer_denominator = excluded.breastcancer_denominator
						 , breastcancer_nanr = excluded.breastcancer_nanr
						 , coronaryarterydisease_numerator = excluded.coronaryarterydisease_numerator
						 , coronaryarterydisease_denominator = excluded.coronaryarterydisease_denominator
						 , coronaryarterydisease_nanr = excluded.coronaryarterydisease_nanr
						 , hiv_numerator = excluded.hiv_numerator
						 , hiv_denominator = excluded.hiv_denominator
						 , hiv_nanr = excluded.hiv_nanr
						 , hivscreening_numerator = excluded.hivscreening_numerator
						 , hivscreening_denominator = excluded.hivscreening_denominator
						 , hivscreening_nanr = excluded.hivscreening_nanr
						 , ivd_ldlcntrl_numerator = excluded.ivd_ldlcntrl_numerator
						 , ivd_ldlcntrl_denominator = excluded.ivd_ldlcntrl_denominator
						 , ivd_ldlcntrl_nanr = excluded.ivd_ldlcntrl_nanr
						 , fluoride_numerator = excluded.fluoride_numerator
						 , fluoride_denominator = excluded.fluoride_denominator
						 , fluoride_nanr = excluded.fluoride_nanr
						 , totalvisits = excluded.totalvisits
						 , sealants6to9_numerator = excluded.sealants6to9_numerator
						 , sealants6to9_denominator = excluded.sealants6to9_denominator
						 , sealants6to9_nanr = excluded.sealants6to9_nanr
						 , treatmentplan_numerator = excluded.treatmentplan_numerator
						 , treatmentplan_denominator = excluded.treatmentplan_denominator
						 , treatmentplan_nanr = excluded.treatmentplan_nanr
						 , cariesrecall_numerator = excluded.cariesrecall_numerator
						 , cariesrecall_denominator = excluded.cariesrecall_denominator
						 , cariesrecall_nanr = excluded.cariesrecall_nanr
						 , riskassess_numerator = excluded.riskassess_numerator
						 , riskassess_denominator = excluded.riskassess_denominator
						 , riskassess_nanr = excluded.riskassess_nanr
						 , oraleval_numerator = excluded.oraleval_numerator
						 , oraleval_denominator = excluded.oraleval_denominator
						 , oraleval_nanr = excluded.oraleval_nanr
						 , topicalfluoride_numerator = excluded.topicalfluoride_numerator
						 , topicalfluoride_denominator = excluded.topicalfluoride_denominator
						 , topicalfluoride_nanr = excluded.topicalfluoride_nanr
						 , sealants10to14_numerator = excluded.sealants10to14_numerator
						 , sealants10to14_denominator = excluded.sealants10to14_denominator
						 , sealants10to14_nanr = excluded.sealants10to14_nanr
						 , goalsetting_numerator = excluded.goalsetting_numerator
						 , goalsetting_denominator = excluded.goalsetting_denominator
						 , goalsetting_nanr = excluded.goalsetting_nanr
						 , goalreview_numerator = excluded.goalreview_numerator
						 , goalreview_denominator = excluded.goalreview_denominator
						 , goalreview_nanr = excluded.goalreview_nanr
						 , recommendations_numerator = excluded.recommendations_numerator
						 , recommendations_denominator = excluded.recommendations_denominator
						 , recommendations_nanr = excluded.recommendations_nanr
						 , recall = excluded.recall
						 , recall_nanr = excluded.recall_nanr
						 , emergency = excluded.emergency
						 , emergency_nanr = excluded.emergency_nanr
						 , comprehensive = excluded.comprehensive
						 , comprehensive_nanr = excluded.comprehensive_nanr
						 , other = excluded.other
						 , other_nanr = excluded.other_nanr
						 , emergencyservices = excluded.emergencyservices
						 , emergencyservices_nanr = excluded.emergencyservices_nanr
						 , oralexams = excluded.oralexams
						 , oralexams_nanr = excluded.oralexams_nanr
						 , prophylaxis = excluded.prophylaxis
						 , prophylaxis_nanr = excluded.prophylaxis_nanr
						 , sealants = excluded.sealants
						 , sealants_nanr = excluded.sealants_nanr
						 , fluoridetreatment = excluded.fluoridetreatment
						 , fluoridetreatment_nanr = excluded.fluoridetreatment_nanr
						 , restorativeservices = excluded.restorativeservices
						 , restorativeservices_nanr = excluded.restorativeservices_nanr
						 , oralsurgery = excluded.oralsurgery
						 , oralsurgery_nanr = excluded.oralsurgery_nanr
						 , rehabservices = excluded.rehabservices
						 , rehabservices_nanr = excluded.rehabservices_nanr
						 , medassistprov_numerator = excluded.medassistprov_numerator
						 , medassistprov_denominator = excluded.medassistprov_denominator
						 , medassistprov_nanr = excluded.medassistprov_nanr
						 , sudmat_numerator = excluded.sudmat_numerator
						 , sudmat_denominator = excluded.sudmat_denominator
						 , sudmat_nanr = excluded.sudmat_nanr
						 , sbirt_numerator = excluded.sbirt_numerator
						 , sbirt_denominator = excluded.sbirt_denominator
						 , sbirt_nanr = excluded.sbirt_nanr
						 , adolescentsud_numerator = excluded.adolescentsud_numerator
						 , adolescentsud_denominator = excluded.adolescentsud_denominator
						 , adolescentsud_nanr = excluded.adolescentsud_nanr
						 , notes = excluded.notes
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						clinicalmeasures.last_modified < excluded.last_modified
					RETURNING clinicalmeasures.clinicid
		)
						
				SELECT count(*)
				FROM insertclinicalmeasures
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: CLINICALMEASURES'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...COVID Table';	
		
		WITH insertcovid AS (
			INSERT INTO final.covid (
				 year
				, clinicid
				, diagnosis_a
				, diagnosis_b
				, test_a
				, test_b
				, notes
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT 
					year
					, clinicid
					, diagnosis_a
					, diagnosis_b
					, test_a
					, test_b
					, notes
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.covid
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  diagnosis_a = excluded.diagnosis_a
						 , diagnosis_b = excluded.diagnosis_b
						 , test_a = excluded.test_a
						 , test_b = excluded.test_b
						 , notes = excluded.notes
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						covid.last_modified < excluded.last_modified
					RETURNING covid.clinicid
		)
						
				SELECT count(*)
				FROM insertcovid
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: COVID'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...EHRDATA Table';	
		
		WITH insertehrdata AS (
			INSERT INTO final.ehrdata (
				 year
				, clinicid
				, q1
				, q1a
				, q1a_vendor
				, q1a_productname
				, q1a_versionnum
				, q1a_listnum
				, q1b
				, q1b_text
				, q1b_prov
				, q1c_yn
				, q1c
				, q1d
				, q1e
				, q2
				, q2_hospitals
				, q2_specialty
				, q2_otherpcp
				, q2_labs
				, q2_hie
				, q2_none
				, q2_other
				, q2_othertext
				, q3
				, q3_patient
				, q3_kiosks
				, q3_secure
				, q3_no
				, q3_other
				, q3_othertext
				, q4
				, q4_hospitals
				, q4_specialty
				, q4_otherpcp
				, q4_none
				, q4_other
				, q4_othertext
				, q5
				, q5_patient
				, q5_kiosks
				, q5_secure
				, q5_qi
				, q5_pophealth
				, q5_progeval
				, q5_research
				, q5_no
				, q5_other
				, q5_othertext
				, q6
				, q7
				, q7_qi
				, q7_pophealth
				, q7_progeval
				, q7_research
				, q7_accountable
				, q7_upstream
				, q7_ihelp
				, q7_recommended
				, q7_prapare
				, q7_wecare
				, q7_wellrx
				, q7_leads
				, q7_no
				, q7_other
				, q7_othertext
				, q7a_food
				, q7a_housing
				, q7a_financial
				, q7a_lacktrans
				, q7b_unfamiliar
				, q7b_lackfunding
				, q7b_lacktraining
				, q7b_inability
				, q7b_notneeded
				, q7b_other
				, q7b_othertext
				, q8
				, q8i
				, q8i_yes
				, q8i_no
				, q8ii
				, q8ii_yes
				, q8ii_no
				, q9
				, q9_yes
				, q9_accountable
				, q9_upstream
				, q9_ihelp
				, q9_recommended
				, q9_prapare
				, q9_wecare
				, q9_wellrx
				, q9_no
				, q9_other
				, q9_othertext
				, q10
				, q10_yes_a
				, q10_yes_b
				, q10_yes_c
				, q10_yes_d
				, q10_yes_e
				, q10_yes_f
				, q10_yesother
				, q11
				, q11_yes
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT 
					 year
					, clinicid
					, q1
					, q1a
					, q1a_vendor
					, q1a_productname
					, q1a_versionnum
					, q1a_listnum
					, q1b
					, q1b_text
					, q1b_prov
					, q1c_yn
					, q1c
					, q1d
					, q1e
					, q2
					, q2_hospitals
					, q2_specialty
					, q2_otherpcp
					, q2_labs
					, q2_hie
					, q2_none
					, q2_other
					, q2_othertext
					, q3
					, q3_patient
					, q3_kiosks
					, q3_secure
					, q3_no
					, q3_other
					, q3_othertext
					, q4
					, q4_hospitals
					, q4_specialty
					, q4_otherpcp
					, q4_none
					, q4_other
					, q4_othertext
					, q5
					, q5_patient
					, q5_kiosks
					, q5_secure
					, q5_qi
					, q5_pophealth
					, q5_progeval
					, q5_research
					, q5_no
					, q5_other
					, q5_othertext
					, q6
					, q7
					, q7_qi
					, q7_pophealth
					, q7_progeval
					, q7_research
					, q7_accountable
					, q7_upstream
					, q7_ihelp
					, q7_recommended
					, q7_prapare
					, q7_wecare
					, q7_wellrx
					, q7_leads
					, q7_no
					, q7_other
					, q7_othertext
					, q7a_food
					, q7a_housing
					, q7a_financial
					, q7a_lacktrans
					, q7b_unfamiliar
					, q7b_lackfunding
					, q7b_lacktraining
					, q7b_inability
					, q7b_notneeded
					, q7b_other
					, q7b_othertext
					, q8
					, q8i
					, q8i_yes
					, q8i_no
					, q8ii
					, q8ii_yes
					, q8ii_no
					, q9
					, q9_yes
					, q9_accountable
					, q9_upstream
					, q9_ihelp
					, q9_recommended
					, q9_prapare
					, q9_wecare
					, q9_wellrx
					, q9_no
					, q9_other
					, q9_othertext
					, q10
					, q10_yes_a
					, q10_yes_b
					, q10_yes_c
					, q10_yes_d
					, q10_yes_e
					, q10_yes_f
					, q10_yesother
					, q11
					, q11_yes
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.ehrdata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  q1 = excluded.q1
						 , q1a = excluded.q1a
						 , q1a_vendor = excluded.q1a_vendor
						 , q1a_productname = excluded.q1a_productname
						 , q1a_versionnum = excluded.q1a_versionnum
						 , q1a_listnum = excluded.q1a_listnum
						 , q1b = excluded.q1b
						 , q1b_text = excluded.q1b_text
						 , q1b_prov = excluded.q1b_prov
						 , q1c_yn = excluded.q1c_yn
						 , q1c = excluded.q1c
						 , q1d = excluded.q1d
						 , q1e = excluded.q1e
						 , q2 = excluded.q2
						 , q2_hospitals = excluded.q2_hospitals
						 , q2_specialty = excluded.q2_specialty
						 , q2_otherpcp = excluded.q2_otherpcp
						 , q2_labs = excluded.q2_labs
						 , q2_hie = excluded.q2_hie
						 , q2_none = excluded.q2_none
						 , q2_other = excluded.q2_other
						 , q2_othertext = excluded.q2_othertext
						 , q3 = excluded.q3
						 , q3_patient = excluded.q3_patient
						 , q3_kiosks = excluded.q3_kiosks
						 , q3_secure = excluded.q3_secure
						 , q3_no = excluded.q3_no
						 , q3_other = excluded.q3_other
						 , q3_othertext = excluded.q3_othertext
						 , q4 = excluded.q4
						 , q4_hospitals = excluded.q4_hospitals
						 , q4_specialty = excluded.q4_specialty
						 , q4_otherpcp = excluded.q4_otherpcp
						 , q4_none = excluded.q4_none
						 , q4_other = excluded.q4_other
						 , q4_othertext = excluded.q4_othertext
						 , q5 = excluded.q5
						 , q5_patient = excluded.q5_patient
						 , q5_kiosks = excluded.q5_kiosks
						 , q5_secure = excluded.q5_secure
						 , q5_qi = excluded.q5_qi
						 , q5_pophealth = excluded.q5_pophealth
						 , q5_progeval = excluded.q5_progeval
						 , q5_research = excluded.q5_research
						 , q5_no = excluded.q5_no
						 , q5_other = excluded.q5_other
						 , q5_othertext = excluded.q5_othertext
						 , q6 = excluded.q6
						 , q7 = excluded.q7
						 , q7_qi = excluded.q7_qi
						 , q7_pophealth = excluded.q7_pophealth
						 , q7_progeval = excluded.q7_progeval
						 , q7_research = excluded.q7_research
						 , q7_accountable = excluded.q7_accountable
						 , q7_upstream = excluded.q7_upstream
						 , q7_ihelp = excluded.q7_ihelp
						 , q7_recommended = excluded.q7_recommended
						 , q7_prapare = excluded.q7_prapare
						 , q7_wecare = excluded.q7_wecare
						 , q7_wellrx = excluded.q7_wellrx
						 , q7_leads = excluded.q7_leads
						 , q7_no = excluded.q7_no
						 , q7_other = excluded.q7_other
						 , q7_othertext = excluded.q7_othertext
						 , q7a_food = excluded.q7a_food
						 , q7a_housing = excluded.q7a_housing
						 , q7a_financial = excluded.q7a_financial
						 , q7a_lacktrans = excluded.q7a_lacktrans
						 , q7b_unfamiliar = excluded.q7b_unfamiliar
						 , q7b_lackfunding = excluded.q7b_lackfunding
						 , q7b_lacktraining = excluded.q7b_lacktraining
						 , q7b_inability = excluded.q7b_inability
						 , q7b_notneeded = excluded.q7b_notneeded
						 , q7b_other = excluded.q7b_other
						 , q7b_othertext = excluded.q7b_othertext
						 , q8 = excluded.q8
						 , q8i = excluded.q8i
						 , q8i_yes = excluded.q8i_yes
						 , q8i_no = excluded.q8i_no
						 , q8ii = excluded.q8ii
						 , q8ii_yes = excluded.q8ii_yes
						 , q8ii_no = excluded.q8ii_no
						 , q9 = excluded.q9
						 , q9_yes = excluded.q9_yes
						 , q9_accountable = excluded.q9_accountable
						 , q9_upstream = excluded.q9_upstream
						 , q9_ihelp = excluded.q9_ihelp
						 , q9_recommended = excluded.q9_recommended
						 , q9_prapare = excluded.q9_prapare
						 , q9_wecare = excluded.q9_wecare
						 , q9_wellrx = excluded.q9_wellrx
						 , q9_no = excluded.q9_no
						 , q9_other = excluded.q9_other
						 , q9_othertext = excluded.q9_othertext
						 , q10 = excluded.q10
						 , q10_yes_a = excluded.q10_yes_a
						 , q10_yes_b = excluded.q10_yes_b
						 , q10_yes_c = excluded.q10_yes_c
						 , q10_yes_d = excluded.q10_yes_d
						 , q10_yes_e = excluded.q10_yes_e
						 , q10_yes_f = excluded.q10_yes_f
						 , q10_yesother = excluded.q10_yesother
						 , q11 = excluded.q11
						 , q11_yes = excluded.q11_yes
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						ehrdata.last_modified < excluded.last_modified
					RETURNING ehrdata.clinicid
		)
						
				SELECT count(*)
				FROM insertehrdata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: EHRDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;




	RAISE NOTICE '...FINANCIALMEASURES Table';	
		
		WITH insertfinancialmeasures AS (
			INSERT INTO final.financialmeasures (
				 year
				, clinicid
				, medical_patient_cost
				, medical_patient_patientvisitcount
				, medical_patient_nanr
				, medical_visit_cost
				, medical_visit_patientvisitcount
				, medical_visit_nanr
				, dental_patient_cost
				, dental_patient_patientvisitcount
				, dental_patient_nanr
				, dental_visit_cost
				, dental_visit_patientvisitcount
				, dental_visit_nanr
				, encperhr_numerator
				, encperhr_denominator
				, encperhr_nanr
				, noshows_numerator
				, noshows_denominator
				, noshows_nanr
				, recallrates_numerator
				, recallrates_denominator
				, recallrates_nanr
				, notes
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT
					 year
					, clinicid
					, medical_patient_cost
					, medical_patient_patientvisitcount
					, medical_patient_nanr
					, medical_visit_cost
					, medical_visit_patientvisitcount
					, medical_visit_nanr
					, dental_patient_cost
					, dental_patient_patientvisitcount
					, dental_patient_nanr
					, dental_visit_cost
					, dental_visit_patientvisitcount
					, dental_visit_nanr
					, encperhr_numerator
					, encperhr_denominator
					, encperhr_nanr
					, noshows_numerator
					, noshows_denominator
					, noshows_nanr
					, recallrates_numerator
					, recallrates_denominator
					, recallrates_nanr
					, notes
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.financialmeasures
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  medical_patient_cost = excluded.medical_patient_cost
						 , medical_patient_patientvisitcount = excluded.medical_patient_patientvisitcount
						 , medical_patient_nanr = excluded.medical_patient_nanr
						 , medical_visit_cost = excluded.medical_visit_cost
						 , medical_visit_patientvisitcount = excluded.medical_visit_patientvisitcount
						 , medical_visit_nanr = excluded.medical_visit_nanr
						 , dental_patient_cost = excluded.dental_patient_cost
						 , dental_patient_patientvisitcount = excluded.dental_patient_patientvisitcount
						 , dental_patient_nanr = excluded.dental_patient_nanr
						 , dental_visit_cost = excluded.dental_visit_cost
						 , dental_visit_patientvisitcount = excluded.dental_visit_patientvisitcount
						 , dental_visit_nanr = excluded.dental_visit_nanr
						 , encperhr_numerator = excluded.encperhr_numerator
						 , encperhr_denominator = excluded.encperhr_denominator
						 , encperhr_nanr = excluded.encperhr_nanr
						 , noshows_numerator = excluded.noshows_numerator
						 , noshows_denominator = excluded.noshows_denominator
						 , noshows_nanr = excluded.noshows_nanr
						 , recallrates_numerator = excluded.recallrates_numerator
						 , recallrates_denominator = excluded.recallrates_denominator
						 , recallrates_nanr = excluded.recallrates_nanr
						 , notes = excluded.notes
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						financialmeasures.last_modified < excluded.last_modified
					RETURNING financialmeasures.clinicid
		)
						
				SELECT count(*)
				FROM insertfinancialmeasures
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: FINANCIALMEASURES'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

	RAISE NOTICE '...MEASURESELECTION Table';	
		
		WITH insertmeasureselection AS (
			INSERT INTO final.measureselection (
				 measureselid
				, year
				, clinicid
				, measure
				, measuretype
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom
			)
				  
				SELECT 
					 measureselid
					, year
					, clinicid
					, measure
					, measuretype
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom
				FROM staging.measureselection
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (measureselid, clinicid, year) 
				DO UPDATE
					SET
						  measure = excluded.measure
						 , measuretype = excluded.measuretype
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						measureselection.last_modified < excluded.last_modified
					RETURNING measureselection.clinicid
		)
						
				SELECT count(*)
				FROM insertmeasureselection
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: MEASURESELECTION'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;


	RAISE NOTICE '...ODEDATA Table';	
		
		WITH insertodedata AS (
			INSERT INTO final.odedata (
				 year
				, clinicid
				, q1a
				, q1b
				, q2
				, q2a1_patients
				, q2a1_specialists
				, q2a2_reatime
				, q2a2_storeforward
				, q2a2_remote
				, q2a2_mobile
				, q2a3_primary
				, q2a3_oral
				, q2a3_mh
				, q2a3_sud
				, q2a3_derm
				, q2a3_chronic
				, q2a3_disaster
				, q2a3_consumer
				, q2a3_prov
				, q2a3_radio
				, q2a3_dietary
				, q2a3_other
				, q2a3_othertext
				, q2b_unfamiliar
				, q2b_policy
				, q2b_lackreimb
				, q2b_inadequate
				, q2b_lackfunding
				, q2b_notneeded
				, q2b_other
				, q2b_othertext
				, q2b_inadequate_cost
				, q2b_inadequate_infrastructure
				, q2b_inadequate_other
				, q2b_inadequate_othertext
				, q2b_policay_lack
				, q2b_policy_cred
				, q2b_policy_privacy
				, q2b_policy_other
				, q2b_policy_othertext
				, q2_yes_a
				, q2_yes_b
				, q2_yes_c
				, q2_yes_d
				, q2_yes_e
				, q2_yesother
				, q2_no
				, q3
				, q4
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom  
			)
				  
				SELECT 
					year
					, clinicid
					, q1a
					, q1b
					, q2
					, q2a1_patients
					, q2a1_specialists
					, q2a2_reatime
					, q2a2_storeforward
					, q2a2_remote
					, q2a2_mobile
					, q2a3_primary
					, q2a3_oral
					, q2a3_mh
					, q2a3_sud
					, q2a3_derm
					, q2a3_chronic
					, q2a3_disaster
					, q2a3_consumer
					, q2a3_prov
					, q2a3_radio
					, q2a3_dietary
					, q2a3_other
					, q2a3_othertext
					, q2b_unfamiliar
					, q2b_policy
					, q2b_lackreimb
					, q2b_inadequate
					, q2b_lackfunding
					, q2b_notneeded
					, q2b_other
					, q2b_othertext
					, q2b_inadequate_cost
					, q2b_inadequate_infrastructure
					, q2b_inadequate_other
					, q2b_inadequate_othertext
					, q2b_policay_lack
					, q2b_policy_cred
					, q2b_policy_privacy
					, q2b_policy_other
					, q2b_policy_othertext
					, q2_yes_a
					, q2_yes_b
					, q2_yes_c
					, q2_yes_d
					, q2_yes_e
					, q2_yesother
					, q2_no
					, q3
					, q4
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom  
				FROM staging.odedata
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year) 
				DO UPDATE
					SET
						  q1a = excluded.q1a
						 , q1b = excluded.q1b
						 , q2 = excluded.q2
						 , q2a1_patients = excluded.q2a1_patients
						 , q2a1_specialists = excluded.q2a1_specialists
						 , q2a2_reatime = excluded.q2a2_reatime
						 , q2a2_storeforward = excluded.q2a2_storeforward
						 , q2a2_remote = excluded.q2a2_remote
						 , q2a2_mobile = excluded.q2a2_mobile
						 , q2a3_primary = excluded.q2a3_primary
						 , q2a3_oral = excluded.q2a3_oral
						 , q2a3_mh = excluded.q2a3_mh
						 , q2a3_sud = excluded.q2a3_sud
						 , q2a3_derm = excluded.q2a3_derm
						 , q2a3_chronic = excluded.q2a3_chronic
						 , q2a3_disaster = excluded.q2a3_disaster
						 , q2a3_consumer = excluded.q2a3_consumer
						 , q2a3_prov = excluded.q2a3_prov
						 , q2a3_radio = excluded.q2a3_radio
						 , q2a3_dietary = excluded.q2a3_dietary
						 , q2a3_other = excluded.q2a3_other
						 , q2a3_othertext = excluded.q2a3_othertext
						 , q2b_unfamiliar = excluded.q2b_unfamiliar
						 , q2b_policy = excluded.q2b_policy
						 , q2b_lackreimb = excluded.q2b_lackreimb
						 , q2b_inadequate = excluded.q2b_inadequate
						 , q2b_lackfunding = excluded.q2b_lackfunding
						 , q2b_notneeded = excluded.q2b_notneeded
						 , q2b_other = excluded.q2b_other
						 , q2b_othertext = excluded.q2b_othertext
						 , q2b_inadequate_cost = excluded.q2b_inadequate_cost
						 , q2b_inadequate_infrastructure = excluded.q2b_inadequate_infrastructure
						 , q2b_inadequate_other = excluded.q2b_inadequate_other
						 , q2b_inadequate_othertext = excluded.q2b_inadequate_othertext
						 , q2b_policay_lack = excluded.q2b_policay_lack
						 , q2b_policy_cred = excluded.q2b_policy_cred
						 , q2b_policy_privacy = excluded.q2b_policy_privacy
						 , q2b_policy_other = excluded.q2b_policy_other
						 , q2b_policy_othertext = excluded.q2b_policy_othertext
						 , q2_yes_a = excluded.q2_yes_a
						 , q2_yes_b = excluded.q2_yes_b
						 , q2_yes_c = excluded.q2_yes_c
						 , q2_yes_d = excluded.q2_yes_d
						 , q2_yes_e = excluded.q2_yes_e
						 , q2_yesother = excluded.q2_yesother
						 , q2_no = excluded.q2_no
						 , q3 = excluded.q3
						 , q4 = excluded.q4
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						odedata.last_modified < excluded.last_modified
					RETURNING odedata.clinicid
		)
						
				SELECT count(*)
				FROM insertodedata
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: ODEDATA'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;




	RAISE NOTICE '...QUARTERCLINICALMEASURES Table';	
		
		WITH insertquarterclinicalmeasures AS (
			INSERT INTO final.quarterclinicalmeasures (
				 year
				, quarter
				, clinicid
				, diabeticpoorcntrlnotest_numerator
				, diabeticpoorcntrlnotest_denominator
				, diabeticpoorcntrlnotest_nanr
				, hypertensive_numerator
				, hypertensive_denominator
				, hypertensive_nanr
				, tobaccousecessation_numerator
				, tobaccousecessation_denominator
				, tobaccousecessation_nanr
				, adultweight_numerator
				, adultweight_denominator
				, adultweight_nanr
				, childweight_numerator
				, childweight_denominator
				, childweight_nanr
				, childimmunizations_numerator
				, childimmunizations_denominator
				, childimmunizations_nanr
				, cervicalcancer_numerator
				, cervicalcancer_denominator
				, cervicalcancer_nanr
				, asthmapharma_numerator
				, asthmapharma_denominator
				, asthmapharma_nanr
				, depression_numerator
				, depression_denominator
				, depression_nanr
				, ivd_numerator
				, ivd_denominator
				, ivd_nanr
				, colorectalcancer_numerator
				, colorectalcancer_denominator
				, colorectalcancer_nanr
				, breastcancer_numerator
				, breastcancer_denominator
				, breastcancer_nanr
				, coronaryarterydisease_numerator
				, coronaryarterydisease_denominator
				, coronaryarterydisease_nanr
				, hiv_numerator
				, hiv_denominator
				, hiv_nanr
				, fluoride_numerator
				, fluoride_denominator
				, fluoride_nanr
				, totalvisits
				, sealants6to9_numerator
				, sealants6to9_denominator
				, sealants6to9_nanr
				, treatmentplan_numerator
				, treatmentplan_denominator
				, treatmentplan_nanr
				, cariesrecall_numerator
				, cariesrecall_denominator
				, cariesrecall_nanr
				, riskassess_numerator
				, riskassess_denominator
				, riskassess_nanr
				, oraleval_numerator
				, oraleval_denominator
				, oraleval_nanr
				, topicalfluoride_numerator
				, topicalfluoride_denominator
				, topicalfluoride_nanr
				, sealants10to14_numerator
				, sealants10to14_denominator
				, sealants10to14_nanr
				, goalsetting_numerator
				, goalsetting_denominator
				, goalsetting_nanr
				, goalreview_numerator
				, goalreview_denominator
				, goalreview_nanr
				, recommendations_numerator
				, recommendations_denominator
				, recommendations_nanr
				, emergencyservices
				, emergencyservices_nanr
				, oralexams
				, oralexams_nanr
				, prophylaxis
				, prophylaxis_nanr
				, sealants
				, sealants_nanr
				, fluoridetreatment
				, fluoridetreatment_nanr
				, restorativeservices
				, restorativeservices_nanr
				, oralsurgery
				, oralsurgery_nanr
				, rehabservices
				, rehabservices_nanr
				, medassistprov_numerator
				, medassistprov_denominator
				, medassistprov_nanr
				, sudmat_numerator
				, sudmat_denominator
				, sudmat_nanr
				, sbirt_numerator
				, sbirt_denominator
				, sbirt_nanr
				, adolescentsud_numerator
				, adolescentsud_denominator
				, adolescentsud_nanr
				, statin_numerator
				, statin_denominator
				, statin_nanr
				, notes
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom 
			)
				  
				SELECT 
					 year
					, quarter
					, clinicid
					, diabeticpoorcntrlnotest_numerator
					, diabeticpoorcntrlnotest_denominator
					, diabeticpoorcntrlnotest_nanr
					, hypertensive_numerator
					, hypertensive_denominator
					, hypertensive_nanr
					, tobaccousecessation_numerator
					, tobaccousecessation_denominator
					, tobaccousecessation_nanr
					, adultweight_numerator
					, adultweight_denominator
					, adultweight_nanr
					, childweight_numerator
					, childweight_denominator
					, childweight_nanr
					, childimmunizations_numerator
					, childimmunizations_denominator
					, childimmunizations_nanr
					, cervicalcancer_numerator
					, cervicalcancer_denominator
					, cervicalcancer_nanr
					, asthmapharma_numerator
					, asthmapharma_denominator
					, asthmapharma_nanr
					, depression_numerator
					, depression_denominator
					, depression_nanr
					, ivd_numerator
					, ivd_denominator
					, ivd_nanr
					, colorectalcancer_numerator
					, colorectalcancer_denominator
					, colorectalcancer_nanr
					, breastcancer_numerator
					, breastcancer_denominator
					, breastcancer_nanr
					, coronaryarterydisease_numerator
					, coronaryarterydisease_denominator
					, coronaryarterydisease_nanr
					, hiv_numerator
					, hiv_denominator
					, hiv_nanr
					, fluoride_numerator
					, fluoride_denominator
					, fluoride_nanr
					, totalvisits
					, sealants6to9_numerator
					, sealants6to9_denominator
					, sealants6to9_nanr
					, treatmentplan_numerator
					, treatmentplan_denominator
					, treatmentplan_nanr
					, cariesrecall_numerator
					, cariesrecall_denominator
					, cariesrecall_nanr
					, riskassess_numerator
					, riskassess_denominator
					, riskassess_nanr
					, oraleval_numerator
					, oraleval_denominator
					, oraleval_nanr
					, topicalfluoride_numerator
					, topicalfluoride_denominator
					, topicalfluoride_nanr
					, sealants10to14_numerator
					, sealants10to14_denominator
					, sealants10to14_nanr
					, goalsetting_numerator
					, goalsetting_denominator
					, goalsetting_nanr
					, goalreview_numerator
					, goalreview_denominator
					, goalreview_nanr
					, recommendations_numerator
					, recommendations_denominator
					, recommendations_nanr
					, emergencyservices
					, emergencyservices_nanr
					, oralexams
					, oralexams_nanr
					, prophylaxis
					, prophylaxis_nanr
					, sealants
					, sealants_nanr
					, fluoridetreatment
					, fluoridetreatment_nanr
					, restorativeservices
					, restorativeservices_nanr
					, oralsurgery
					, oralsurgery_nanr
					, rehabservices
					, rehabservices_nanr
					, medassistprov_numerator
					, medassistprov_denominator
					, medassistprov_nanr
					, sudmat_numerator
					, sudmat_denominator
					, sudmat_nanr
					, sbirt_numerator
					, sbirt_denominator
					, sbirt_nanr
					, adolescentsud_numerator
					, adolescentsud_denominator
					, adolescentsud_nanr
					, statin_numerator
					, statin_denominator
					, statin_nanr
					, notes
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom 
				FROM staging.quarterclinicalmeasures
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year, quarter) 
				DO UPDATE
					SET
						  diabeticpoorcntrlnotest_numerator = excluded.diabeticpoorcntrlnotest_numerator
						 , diabeticpoorcntrlnotest_denominator = excluded.diabeticpoorcntrlnotest_denominator
						 , diabeticpoorcntrlnotest_nanr = excluded.diabeticpoorcntrlnotest_nanr
						 , hypertensive_numerator = excluded.hypertensive_numerator
						 , hypertensive_denominator = excluded.hypertensive_denominator
						 , hypertensive_nanr = excluded.hypertensive_nanr
						 , tobaccousecessation_numerator = excluded.tobaccousecessation_numerator
						 , tobaccousecessation_denominator = excluded.tobaccousecessation_denominator
						 , tobaccousecessation_nanr = excluded.tobaccousecessation_nanr
						 , adultweight_numerator = excluded.adultweight_numerator
						 , adultweight_denominator = excluded.adultweight_denominator
						 , adultweight_nanr = excluded.adultweight_nanr
						 , childweight_numerator = excluded.childweight_numerator
						 , childweight_denominator = excluded.childweight_denominator
						 , childweight_nanr = excluded.childweight_nanr
						 , childimmunizations_numerator = excluded.childimmunizations_numerator
						 , childimmunizations_denominator = excluded.childimmunizations_denominator
						 , childimmunizations_nanr = excluded.childimmunizations_nanr
						 , cervicalcancer_numerator = excluded.cervicalcancer_numerator
						 , cervicalcancer_denominator = excluded.cervicalcancer_denominator
						 , cervicalcancer_nanr = excluded.cervicalcancer_nanr
						 , asthmapharma_numerator = excluded.asthmapharma_numerator
						 , asthmapharma_denominator = excluded.asthmapharma_denominator
						 , asthmapharma_nanr = excluded.asthmapharma_nanr
						 , depression_numerator = excluded.depression_numerator
						 , depression_denominator = excluded.depression_denominator
						 , depression_nanr = excluded.depression_nanr
						 , ivd_numerator = excluded.ivd_numerator
						 , ivd_denominator = excluded.ivd_denominator
						 , ivd_nanr = excluded.ivd_nanr
						 , colorectalcancer_numerator = excluded.colorectalcancer_numerator
						 , colorectalcancer_denominator = excluded.colorectalcancer_denominator
						 , colorectalcancer_nanr = excluded.colorectalcancer_nanr
						 , breastcancer_numerator = excluded.breastcancer_numerator
						 , breastcancer_denominator = excluded.breastcancer_denominator
						 , breastcancer_nanr = excluded.breastcancer_nanr
						 , coronaryarterydisease_numerator = excluded.coronaryarterydisease_numerator
						 , coronaryarterydisease_denominator = excluded.coronaryarterydisease_denominator
						 , coronaryarterydisease_nanr = excluded.coronaryarterydisease_nanr
						 , hiv_numerator = excluded.hiv_numerator
						 , hiv_denominator = excluded.hiv_denominator
						 , hiv_nanr = excluded.hiv_nanr
						 , fluoride_numerator = excluded.fluoride_numerator
						 , fluoride_denominator = excluded.fluoride_denominator
						 , fluoride_nanr = excluded.fluoride_nanr
						 , totalvisits = excluded.totalvisits
						 , sealants6to9_numerator = excluded.sealants6to9_numerator
						 , sealants6to9_denominator = excluded.sealants6to9_denominator
						 , sealants6to9_nanr = excluded.sealants6to9_nanr
						 , treatmentplan_numerator = excluded.treatmentplan_numerator
						 , treatmentplan_denominator = excluded.treatmentplan_denominator
						 , treatmentplan_nanr = excluded.treatmentplan_nanr
						 , cariesrecall_numerator = excluded.cariesrecall_numerator
						 , cariesrecall_denominator = excluded.cariesrecall_denominator
						 , cariesrecall_nanr = excluded.cariesrecall_nanr
						 , riskassess_numerator = excluded.riskassess_numerator
						 , riskassess_denominator = excluded.riskassess_denominator
						 , riskassess_nanr = excluded.riskassess_nanr
						 , oraleval_numerator = excluded.oraleval_numerator
						 , oraleval_denominator = excluded.oraleval_denominator
						 , oraleval_nanr = excluded.oraleval_nanr
						 , topicalfluoride_numerator = excluded.topicalfluoride_numerator
						 , topicalfluoride_denominator = excluded.topicalfluoride_denominator
						 , topicalfluoride_nanr = excluded.topicalfluoride_nanr
						 , sealants10to14_numerator = excluded.sealants10to14_numerator
						 , sealants10to14_denominator = excluded.sealants10to14_denominator
						 , sealants10to14_nanr = excluded.sealants10to14_nanr
						 , goalsetting_numerator = excluded.goalsetting_numerator
						 , goalsetting_denominator = excluded.goalsetting_denominator
						 , goalsetting_nanr = excluded.goalsetting_nanr
						 , goalreview_numerator = excluded.goalreview_numerator
						 , goalreview_denominator = excluded.goalreview_denominator
						 , goalreview_nanr = excluded.goalreview_nanr
						 , recommendations_numerator = excluded.recommendations_numerator
						 , recommendations_denominator = excluded.recommendations_denominator
						 , recommendations_nanr = excluded.recommendations_nanr
						 , emergencyservices = excluded.emergencyservices
						 , emergencyservices_nanr = excluded.emergencyservices_nanr
						 , oralexams = excluded.oralexams
						 , oralexams_nanr = excluded.oralexams_nanr
						 , prophylaxis = excluded.prophylaxis
						 , prophylaxis_nanr = excluded.prophylaxis_nanr
						 , sealants = excluded.sealants
						 , sealants_nanr = excluded.sealants_nanr
						 , fluoridetreatment = excluded.fluoridetreatment
						 , fluoridetreatment_nanr = excluded.fluoridetreatment_nanr
						 , restorativeservices = excluded.restorativeservices
						 , restorativeservices_nanr = excluded.restorativeservices_nanr
						 , oralsurgery = excluded.oralsurgery
						 , oralsurgery_nanr = excluded.oralsurgery_nanr
						 , rehabservices = excluded.rehabservices
						 , rehabservices_nanr = excluded.rehabservices_nanr
						 , medassistprov_numerator = excluded.medassistprov_numerator
						 , medassistprov_denominator = excluded.medassistprov_denominator
						 , medassistprov_nanr = excluded.medassistprov_nanr
						 , sudmat_numerator = excluded.sudmat_numerator
						 , sudmat_denominator = excluded.sudmat_denominator
						 , sudmat_nanr = excluded.sudmat_nanr
						 , sbirt_numerator = excluded.sbirt_numerator
						 , sbirt_denominator = excluded.sbirt_denominator
						 , sbirt_nanr = excluded.sbirt_nanr
						 , adolescentsud_numerator = excluded.adolescentsud_numerator
						 , adolescentsud_denominator = excluded.adolescentsud_denominator
						 , adolescentsud_nanr = excluded.adolescentsud_nanr
						 , statin_numerator = excluded.statin_numerator
						 , statin_denominator = excluded.statin_denominator
						 , statin_nanr = excluded.statin_nanr
						 , notes = excluded.notes
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						quarterclinicalmeasures.last_modified < excluded.last_modified
					RETURNING quarterclinicalmeasures.clinicid
		)
						
				SELECT count(*)
				FROM insertquarterclinicalmeasures
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: QUARTERCLINICALMEASURES'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;







RAISE NOTICE '...QUARTERFINANCIALMEASURE Table';	
		
		WITH insertquarterfinancialmeasures AS (
			INSERT INTO final.quarterfinancialmeasures (
				 year
				, quarter
				, clinicid
				, medical_patient_cost
				, medical_patient_patientvisitcount
				, medical_patient_nanr
				, medical_visit_cost
				, medical_visit_patientvisitcount
				, medical_visit_nanr
				, dental_patient_cost
				, dental_patient_patientvisitcount
				, dental_patient_nanr
				, dental_visit_cost
				, dental_visit_patientvisitcount
				, dental_visit_nanr
				, encperhr_numerator
				, encperhr_denominator
				, encperhr_nanr
				, noshows_numerator
				, noshows_denominator
				, noshows_nanr
				, recallrates_numerator
				, recallrates_denominator
				, recallrates_nanr
				, notes
				, data_extension
				, status
				, created
				, created_bywhom
				, last_modified
				, last_modified_bywhom   
			)
				  
				SELECT 
					 year
					, quarter
					, clinicid
					, medical_patient_cost
					, medical_patient_patientvisitcount
					, medical_patient_nanr
					, medical_visit_cost
					, medical_visit_patientvisitcount
					, medical_visit_nanr
					, dental_patient_cost
					, dental_patient_patientvisitcount
					, dental_patient_nanr
					, dental_visit_cost
					, dental_visit_patientvisitcount
					, dental_visit_nanr
					, encperhr_numerator
					, encperhr_denominator
					, encperhr_nanr
					, noshows_numerator
					, noshows_denominator
					, noshows_nanr
					, recallrates_numerator
					, recallrates_denominator
					, recallrates_nanr
					, notes
					, data_extension
					, status
					, created
					, created_bywhom
					, last_modified
					, last_modified_bywhom   
				FROM staging.quarterfinancialmeasures
				WHERE clinicid = varclinicid
				AND year = varyear
				
				ON CONFLICT (clinicid, year, quarter) 
				DO UPDATE
					SET
						  medical_patient_cost = excluded.medical_patient_cost
						 , medical_patient_patientvisitcount = excluded.medical_patient_patientvisitcount
						 , medical_patient_nanr = excluded.medical_patient_nanr
						 , medical_visit_cost = excluded.medical_visit_cost
						 , medical_visit_patientvisitcount = excluded.medical_visit_patientvisitcount
						 , medical_visit_nanr = excluded.medical_visit_nanr
						 , dental_patient_cost = excluded.dental_patient_cost
						 , dental_patient_patientvisitcount = excluded.dental_patient_patientvisitcount
						 , dental_patient_nanr = excluded.dental_patient_nanr
						 , dental_visit_cost = excluded.dental_visit_cost
						 , dental_visit_patientvisitcount = excluded.dental_visit_patientvisitcount
						 , dental_visit_nanr = excluded.dental_visit_nanr
						 , encperhr_numerator = excluded.encperhr_numerator
						 , encperhr_denominator = excluded.encperhr_denominator
						 , encperhr_nanr = excluded.encperhr_nanr
						 , noshows_numerator = excluded.noshows_numerator
						 , noshows_denominator = excluded.noshows_denominator
						 , noshows_nanr = excluded.noshows_nanr
						 , recallrates_numerator = excluded.recallrates_numerator
						 , recallrates_denominator = excluded.recallrates_denominator
						 , recallrates_nanr = excluded.recallrates_nanr
						 , notes = excluded.notes
						 , data_extension = excluded.data_extension
						 , status = excluded.status
						 , created = excluded.created
						 , created_bywhom = excluded.created_bywhom
						 , last_modified = excluded.last_modified
						 , last_modified_bywhom = excluded.last_modified_bywhom
					WHERE
						quarterfinancialmeasures.last_modified < excluded.last_modified
					RETURNING quarterfinancialmeasures.clinicid
		)
						
				SELECT count(*)
				FROM insertquarterfinancialmeasures
				INTO processedRecordCount
			;
				RAISE NOTICE 'Completed: QUARTERFINANCIALMEASURE'
					USING DETAIL = 
						concat_ws(
							detailStatementDelimiter
							, (SELECT 'Inserted:' || processedRecordCount)
						)
			;

--The END

	RAISE NOTICE '**All Tables Loaded!**';



-----------------------------------------------------------
---------------------  CLEANUP  ---------------------------
----- Deletes from the working schema any data that  ------
--- has not been modified in 24 calendar months or more ---
-----------------------------------------------------------

CREATE OR REPLACE FUNCTION cleanup(
	varYear float, varClinicID uuid
	)
    RETURNS VOID
	LANGUAGE 'plpgsql'
    --COST 100
    VOLATILE 
    --ROWS 1000
AS
$$
DECLARE
    cleanuptables record;
	
BEGIN
    FOR cleanuptables IN SELECT
			normaltables.table_catalog databasename
			, normaltables.table_name tablename
			, normaltables.table_schema localschemaname
		FROM information_schema."tables" normaltables
		WHERE
			normaltables.table_catalog = current_database()::VARCHAR
			AND normaltables.table_type = 'BASE TABLE'
			AND NormalTables.table_schema = 'staging'
			AND NormalTables.table_name NOT IN ('admin', 'specialty', 'county', 'definitions', 'rules', 'validation_results',
												'clinic', 'cliniccontact', 'desigandmember',
												'clinicbuild', 'statustype', 'tasktype') --exclude these

    LOOP 
    		
		EXECUTE
						
			'DELETE FROM ' || cleanuptables.localschemaname || '.'	|| cleanuptables.tablename
				|| ' WHERE ' 
					--older than 24 calendar months
					||'date(last_modified) < (date(now())-interval ''24 months'')'
					--||'AND year = '||varyear
					||'AND clinicid = '||quote_literal(varclinicid)
	
			;
	RAISE NOTICE 'Cleaning up %...',cleanuptables.tablename;
	
    END LOOP;
	RAISE NOTICE '**Clean Up Is Complete!**';
		
			
			
END;
$$;

	SELECT cleanup(varyear,varclinicid) INTO cleanup;
	
	UPDATE staging.clinicbuild SET 
					task_id = 0, task_name = 'No Task'
				WHERE year = varyear
				AND clinicid = varclinicid;
			

----------------------------------------------------

/*
	
	--Successful Execution
	RETURN QUERY 
		SELECT TRUE prelimExecutionSuccessFlag, prelimExecutionDetails;

	--Failure
	EXCEPTION
		WHEN unique_violation
		THEN
			GET STACKED DIAGNOSTICS
				  prelimExecutionDetails = MESSAGE_TEXT				
				, exceptionMessageText = MESSAGE_TEXT
				, exceptionTableName = TABLE_NAME
				, exceptionDetail = PG_EXCEPTION_DETAIL
			;

			RAISE NOTICE 'Unique Constraint Violated - Bad Data - Table: % Record: **** % **** Message: % (Logged)'
				, exceptionTableName, exceptionDetail, exceptionMessageText
			;

			--The Return is displayed by the Application
			RETURN QUERY
				SELECT FALSE, 'Processing Failed - Input data was not unique.' || prelimExecutionDetails;

		--Generic SQL Failure
		WHEN OTHERS 
		THEN
			GET STACKED DIAGNOSTICS
				prelimExecutionDetails = MESSAGE_TEXT				
				, exceptionSQLState = RETURNED_SQLSTATE
				, exceptionMessageText = MESSAGE_TEXT
				, exceptionTableName = TABLE_NAME
				, exceptionColumnName = COLUMN_NAME
				, exceptionDetail = PG_EXCEPTION_DETAIL
			;

			RAISE NOTICE 'Clinical Data ETL has failed. %, %, %, %, %'
				, exceptionMessageText, exceptionDetail, exceptionTableName, exceptionColumnName, exceptionSQLState
			;

			RETURN QUERY
				SELECT FALSE, 'Processing Failed - Details: ' || prelimExecutionDetails;
	
	RETURN;

*/

END
$BODY$;
	
