#!/usr/bin/python
import MySQLdb
db = MySQLdb.connect("localhost","root","mips","mips")
cursor = db.cursor()

query = """truncate table mips.cpia_actvty_prfmnc_scre_dtl"""

sql = "select  tin,\
        npi,\
        pgm_yr_id,\
        CPIA_SUBMS_MTHD_ID,\
        CPIA_ACTVTY_CD,\
        CPIA_ACTVTY_WT,\
        case when CPIA_ACTVTY_WT = 10 then  10 else 20 end cpia_actvty_adjst_pts ,\
        CPIA_SUBMSN_DATA_ID  \
from mips.cpia_actv_submsn_data_stng"


try:
    cursor.execute(query)
    db.commit()
    
    cursor.execute(sql)
    results = cursor.fetchall()

    
    number = 0
    for row in results:
        
       tin                    = row[0]
       npi                    = row[1]
       pgm_yr_id              = row[2]
       cpia_subms_mthd_id         = row[3]
       cpia_actvty_cd         = row[4]     
       cpia_actvty_wt         = row[5]
       cpia_actvty_adjst_pts  = row[6]
       cpia_submsn_data_id    = row[7]         
       number = number + 1
       
      
      
       cursor.execute('''INSERT into mips.cpia_actvty_prfmnc_scre_dtl (tin, npi, pgm_yr_id,cpia_subms_mthd_id,cpia_actvty_cd,cpia_actvty_wt, cpia_actvty_adjst_pts, cpia_submsn_data_id,CPIA_PRFMNC_SCRE_ID)
                  values (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',(tin, npi, pgm_yr_id,cpia_subms_mthd_id,cpia_actvty_cd,cpia_actvty_wt, cpia_actvty_adjst_pts, cpia_submsn_data_id,number))
        
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
