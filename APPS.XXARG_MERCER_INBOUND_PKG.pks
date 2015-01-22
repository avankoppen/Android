CREATE OR REPLACE PACKAGE APPS.xxarg_mercer_inbound_pkg AUTHID CURRENT_USER AS
--
--
/***********************************************************************************************************************
$HEADER:  XXARG_MERCER_INBOUND_PKG.PKS 115.0 09-OCT-2009 MURTHY $
             COPYRIGHT (C) 2009, BY WENDY'S ARBY'S GROUP

PACKAGE NAME	: XXARG_MERCER_INBOUND_PKG
AUTHOR		: MURTHY
DESCRIPTION	: THIS PACKAGE IS USED TO CREATE BEE FOR MERCER HEALTH AND CONTRIBUTION 
CHANGED BY	CHANGE DATE	VERSION	COMMENTS
RAM VANAGALA 5-NOV-2009	1.1     MADE CHAGES TO UPDATE ELIGIABLITY DATE 
                                MADE CHANGES TO GET SEQUENCE DC004
								
RAM VANAGALA 16-DEC-2009 1.2 MADE CHANGES TO THE EFFECTIVE DATE OF ELEMENTS ENTRIES. IF IT LESS
THAN SYSDATE THEN SYSDATE ELSE EFFECTIVE_DATE	
*********************************************************************************************/
PROCEDURE process_hb_file (     errbuf                    OUT           VARCHAR2    
                                ,retcode                  OUT           NUMBER
                                ,p_business_group_id      IN            NUMBER
								,p_move_flag              IN            VARCHAR2
                                ,p_rerun_flag             IN            VARCHAR2
								,p_open_enrollment_flag        IN            VARCHAR2
                                ,p_debug_flag             IN            VARCHAR2
                          );
--
--
PROCEDURE process_hsa_file (     errbuf                    OUT           VARCHAR2    
                                ,retcode                  OUT           NUMBER
                                ,p_business_group_id      IN            NUMBER
								,p_move_flag              IN            VARCHAR2
                                ,p_rerun_flag             IN            VARCHAR2
								,p_open_enrollment_flag        IN            VARCHAR2
                                ,p_debug_flag             IN            VARCHAR2
                          );
--
--
PROCEDURE process_dc_file (     errbuf                    OUT           VARCHAR2    
                                ,retcode                  OUT           NUMBER
                                ,p_business_group_id      IN            NUMBER
								,p_move_flag              IN            VARCHAR2
                                ,p_rerun_flag             IN            VARCHAR2
                                ,p_debug_flag             IN            VARCHAR2
                          );
--
--



l_rec_val XXARG_HR_HSA_PKG.gtbl_load_bypass_tbl;



END xxarg_mercer_inbound_pkg; 
/

