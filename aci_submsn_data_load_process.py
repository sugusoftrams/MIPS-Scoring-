#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()

query = """truncate table mips.aci_peformance_scre_dtl"""

sql = "select  qsd.aci_submsn_mthd_id,\
               qsd.aci_msr_rfrnc_id,\
               qsd.tin,\
               qsd.npi,\
               qsd.msr_prfmnc_rate,\
               case when qsd.ACI_OBJCTV_CD = 'CCPE' then  15\
               when qsd.ACI_OBJCTV_CD = 'eRx' then     5\
               when qsd.ACI_OBJCTV_CD = 'CDRR' then    5\
               when qsd.ACI_OBJCTV_CD = 'HIE' then     20\
	       when qsd.ACI_OBJCTV_CD = 'PEA' then 15\
            end msr_base_pts,\
            round(qsd.MSR_PRFMNC_RATE/10) msr_prfmnc_pts, qsd.pgm_yr_id,qsd.aci_SUBMSN_DATA_ID from   mips.aci_msr_submsn_data_stng  qsd, mips.aci_msr_rfrnc qmr  where qsd.aci_msr_rfrnc_id = qmr.aci_msr_rfrnc_id"                                          
                          
      
try:

    cursor.execute(query)
    db.commit()
    
    cursor.execute(sql)
    results = cursor.fetchall()

    
    number = 0
    
    for row in results:

       aci_submsn_mthd_id    = row[0]
       aci_msr_rfrnc_id      = row[1]
       tin                   = row[2]
       npi                   = row[3]
       msr_prfmnc_rate       = row[4]
       msr_base_pts          = row[5]
       msr_prfmnc_pts        = row[6]     
       pgm_yr_id             = row[7]
       aci_submsn_data_id    = row[8]      
       number = number + 1

      
      
       cursor.execute('''INSERT into mips.aci_peformance_scre_dtl ( aci_submsn_mthd_id,aci_msr_rfrnc_id,tin, npi,msr_prfmnc_rate,msr_base_pts,msr_prfmnc_pts,pgm_yr_id,aci_submsn_data_id,ACI_PRFMNC_ID)
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(aci_submsn_mthd_id,aci_msr_rfrnc_id,tin, npi,msr_prfmnc_rate,msr_base_pts,msr_prfmnc_pts,pgm_yr_id,aci_submsn_data_id,number))
        
    ##   print "tin=%s,npi=%s,msr_rfrnc_id=%d,pgm_yr_id=%s,submsn_mthd_id=%d,msr_type_cd=%d,msr_prfmnc_rate=%d,msr_wt=%d,msr_pts=%d,msr_adjst_pts=%d,msr_tot_psbl_pts=%d" % \
      ##       ( TIN , NPI , MSR_RFRNC_ID ,PGM_YR_ID , SUBMSN_MTHD_ID, msr_type_cd,msr_prfmnc_rate,msr_wt,msr_pts,msr_adjst_pts,msr_tot_psbl_t )
       
    db.commit()
except RuntimeError as e:
    print e
except TypeError as te:
    print te
except NameError as ne:
    print ne
    
db.close()
