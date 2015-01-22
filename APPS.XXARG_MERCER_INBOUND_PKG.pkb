CREATE OR REPLACE PACKAGE BODY APPS.XXARG_MERCER_INBOUND_PKG AS

/***********************************************************************************************************************
$HEADER:  XXARG_MERCER_INBOUND_PKG.PKS 115.0 09-OCT-2009 MURTHY $
             COPYRIGHT (C) 2009, BY WENDY'S ARBY'S GROUP

PACKAGE NAME    : XXARG_MERCER_INBOUND_PKG
AUTHOR        : MURTHY
DESCRIPTION    : THIS PACKAGE IS USED TO CREATE BEE FOR MERCER HEALTH AND CONTRIBUTION
CHANGED BY    CHANGE DATE    VERSION    COMMENTS
RAM VANAGALA 5-NOV-2009    1.1     MADE CHAGES TO UPDATE ELIGIABLITY DATE
                                MADE CHANGES TO GET SEQUENCE DC004
RAM VANAGALA 16-DEC-2009 1.2 MADE CHANGES TO THE EFFECTIVE DATE OF ELEMENTS ENTRIES. IF IT LESS
THAN SYSDATE THEN SYSDATE ELSE EFFECTIVE_DATE

RAM VANAGALA  28-JAN-2009 ADDED FUNCTION TO TRANSFER FILE.
*********************************************************************************************/
--
   g_debug_flag    CHAR (1)  := 'N';
   g_end_program   EXCEPTION;

--
--
   FUNCTION debug_on
      RETURN NUMBER
   IS
--
   BEGIN
      IF g_debug_flag = 'Y'
      THEN
         RETURN (1);
      ELSE
         RETURN (NULL);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN (NULL);
   END debug_on;

   FUNCTION file_transfer (l_in_file_name VARCHAR2, l_out_file_name VARCHAR2)
      RETURN NUMBER
   IS
      lc_dir             VARCHAR2 (127);
      lc_in_filename     VARCHAR2 (150);
      lc_out_filename    VARCHAR2 (150);
      ln_request_id      PLS_INTEGER;
      ln_rows            PLS_INTEGER;
      l_call_status      BOOLEAN;
      l_num_interval     NUMBER         := 20;
      l_num_max_time     NUMBER         := 0;
      l_chr_phase        VARCHAR2 (80);
      l_chr_status       VARCHAR2 (80);
      l_chr_del_phase    VARCHAR2 (80);
      l_chr_del_status   VARCHAR2 (80);
      l_chr_msg          VARCHAR2 (80);
   BEGIN
      FOR lrec_dir IN (SELECT directory_path
                         FROM all_directories
                        WHERE directory_name = xxarg_hr_util_pkg.gdir_hr_in)
      LOOP
         lc_dir := lrec_dir.directory_path;
         lc_in_filename := l_in_file_name;
         lc_out_filename := l_out_file_name;
      END LOOP get_dir;

      ln_request_id :=
         fnd_request.submit_request
            (application      => 'XXARG',
             PROGRAM          => 'XXARGFM2SFTP',
             argument1        => '/share/applmgr/product/11i/xxarg/11.5.0/data/hr/inbound',
             argument2        => lc_in_filename,
             argument3        => '',
             argument4        => lc_out_filename,
             argument5        => 'hr',
             argument6        => 'N',
             argument7        => 'N',
             argument8        => lc_dir
                            --THIS NEEDS TO BE YES TO SEND TO SFTP SERVER TODO
            );
      COMMIT;

      IF ln_request_id = 0
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Request not submitted: '
                            || ' Error : '
                            || fnd_message.get
                           );
         RETURN 1;
      ELSE
         fnd_file.put_line (fnd_file.LOG,
                            'Submitted Request ' || ln_request_id
                           );
         l_call_status :=
            fnd_concurrent.wait_for_request (ln_request_id,
                                             l_num_interval,
                                             l_num_max_time,
                                             l_chr_phase,
                                             l_chr_status,
                                             l_chr_del_phase,
                                             l_chr_del_status,
                                             l_chr_msg
                                            );
         COMMIT;
      END IF;

      IF UPPER (l_chr_phase) = 'COMPLETED' AND UPPER (l_chr_status) = 'NORMAL'
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Request # '
                            || ln_request_id
                            || ' Completed with Phase '
                            || l_chr_del_phase
                            || ', Status '
                            || l_chr_del_status
                           );
         RETURN 0;
      ELSE
         RETURN 1;
         fnd_file.put_line
                  (fnd_file.LOG,
                      'File Mover program "XXARGFM2SFTP2" failed to submit. '
                   || CHR (10)
                   || fnd_message.get ()
                  );
      END IF;
   --RETURN THE REQUEST ID
   END file_transfer;

--
--

   --
--
   PROCEDURE generate_hb_error_report
   IS
--
      l_count   NUMBER := 0;
   BEGIN
      --
      --  WRITE ERROR REPORT TO THE OUTPUT FILE.
      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line
          (2,
           '                    ARG-Mercer HB Inbound Interface Error Report'
          );
      fnd_file.put_line
           (2,
            '                    ********************************************'
           );
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            'Date/Time: '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                        );
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            RPAD ('TAX_ID_NUMBER', 20, ' ')
                         || RPAD ('EMPLOYEE_NUMBER', 20, ' ')
                         || RPAD ('DEDUCTION_CODE', 20, ' ')
                         || LPAD ('DEDUCTION_AMOUNT', 20, ' ')
                         || RPAD ('     ERROR_MESSAGE', 100, ' ')
                        );
      fnd_file.put_line (2, RPAD ('*', 100, '*'));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');

      --
      FOR ERRORS IN (SELECT *
                       FROM xxarg_mercer_inbound_hb_stage A
                      WHERE process_flag = 'E')
      LOOP
         --
         l_count := l_count + 1;
         fnd_file.put_line (2,
                               RPAD (ERRORS.tax_id_number, 20, ' ')
                            || RPAD (ERRORS.employee_number, 20, ' ')
                            || RPAD (ERRORS.deduction_code, 20, ' ')
                            || LPAD (ERRORS.deduction_amount, 20, ' ')
                            || '     '
                            || ERRORS.process_message
                           );
      --
      END LOOP;

      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, 'Total Error Count: ' || TO_CHAR (l_count));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE g_end_program;
   END generate_hb_error_report;

   PROCEDURE generate_hb_end_report
   IS
--
      l_count   NUMBER := 0;
   BEGIN
      fnd_file.put_line (2,
                            RPAD ('TAX_ID_NUMBER', 20, ' ')
                         || RPAD ('EMPLOYEE_NUMBER', 20, ' ')
                         || RPAD ('DEDUCTION_CODE', 20, ' ')
                         || LPAD ('DEDUCTION_AMOUNT', 20, ' ')
                         || RPAD ('     EFFECTIVE_DATE', 20, ' ')
                         || RPAD ('EMPLOYEE_TYPE', 20, ' ')
                        );
      fnd_file.put_line (2, RPAD ('*', 120, '*'));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      l_count := 0;

      FOR ERRORS IN
         (SELECT tax_id_number, A.employee_number, deduction_code,
                 deduction_amount, effective_date,
                 ppt.user_person_type employee_type
            FROM xxarg_mercer_inbound_hb_stage A,
                 per_all_people_f papf,
                 per_person_types ppt
           WHERE process_flag = 'Y'
             AND pbl_batch_line_id = 1
             AND papf.person_type_id = ppt.person_type_id
             AND LTRIM (papf.employee_number, 0) =
                                           LTRIM (TRIM (A.employee_number), 0)
             AND TRUNC (A.effective_date)
                    BETWEEN TRUNC (papf.effective_start_date)
                        AND TRUNC (papf.effective_end_date)
             AND A.request_id = fnd_global.conc_request_id)
      LOOP
         --
         l_count := l_count + 1;
         fnd_file.put_line (2,
                               RPAD (ERRORS.tax_id_number, 20, ' ')
                            || RPAD (ERRORS.employee_number, 20, ' ')
                            || RPAD (ERRORS.deduction_code, 20, ' ')
                            || LPAD (ERRORS.deduction_amount, 20, ' ')
                            || '     '
                            || RPAD (ERRORS.effective_date, 20, ' ')
                            || RPAD (ERRORS.employee_type, 20, ' ')
                           );
      --
      END LOOP;

      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            'Total Element Entries End dated: '
                         || TO_CHAR (l_count)
                        );
      fnd_file.put_line (2, ' ');
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE g_end_program;
   END;

   PROCEDURE generate_dc_end_report
   IS
--
      l_count   NUMBER := 0;
   BEGIN
      fnd_file.put_line (2,
                            RPAD ('TAX_ID_NUMBER', 20, ' ')
                         || RPAD ('EMPLOYEE_NUMBER', 20, ' ')
                         || RPAD ('     EFFECTIVE_DATE', 20, ' ')
                         || RPAD ('EMPLOYEE_TYPE', 20, ' ')
                        );
      fnd_file.put_line (2, RPAD ('*', 120, '*'));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      l_count := 0;

      FOR ERRORS IN
         (SELECT employee_ssn, A.employee_number, entry_date effective_date,
                 ppt.user_person_type employee_type
            FROM xxarg_mercer_dc001_stage A,
                 per_all_people_f papf,
                 per_person_types ppt
           WHERE process_flag = 'Y'
             AND pbl_batch_line_id = 1
             AND papf.person_type_id = ppt.person_type_id
             AND LTRIM (papf.employee_number, 0) =
                                           LTRIM (TRIM (A.employee_number), 0)
             AND TRUNC (TO_DATE (entry_date, 'MMDDYYYY'))
                    BETWEEN (papf.effective_start_date)
                        AND (papf.effective_end_date)
             AND A.request_id = fnd_global.conc_request_id)
      LOOP
         --
         l_count := l_count + 1;
         fnd_file.put_line (2,
                               RPAD (ERRORS.employee_ssn, 20, ' ')
                            || RPAD (ERRORS.employee_number, 20, ' ')
                            || '     '
                            || RPAD (ERRORS.effective_date, 20, ' ')
                            || RPAD (ERRORS.employee_type, 20, ' ')
                           );
      --
      END LOOP;

      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            'Total Element Entries End dated: '
                         || TO_CHAR (l_count)
                        );
      fnd_file.put_line (2, ' ');
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE g_end_program;
   END;

--
--
   PROCEDURE generate_dc_error_report
   IS
--
      l_count   NUMBER := 0;
   BEGIN
      --
      --  WRITE ERROR REPORT TO THE OUTPUT FILE.
      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line
          (2,
           '                    ARG-Mercer DC Inbound Interface Error Report'
          );
      fnd_file.put_line
           (2,
            '                    ********************************************'
           );
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            'Date/Time: '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                        );
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, '****************************** ');
      fnd_file.put_line (2, 'DEFERRAL PERCENT UPDATE ERRORS ');
      fnd_file.put_line (2, '****************************** ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            RPAD ('TAX_ID_NUMBER', 20, ' ')
                         || RPAD ('EMPLOYEE_NUMBER', 20, ' ')
                         || RPAD ('ERROR_MESSAGE', 100, ' ')
                        );
      fnd_file.put_line (2, RPAD ('*', 60, '*'));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');

      --
      FOR ERRORS IN (SELECT *
                       FROM xxarg_mercer_dc001_stage A
                      WHERE process_flag = 'E')
      LOOP
         --
         l_count := l_count + 1;
         fnd_file.put_line (2,
                               RPAD (ERRORS.employee_ssn, 20, ' ')
                            || RPAD (ERRORS.employee_number, 20, ' ')
                            || ERRORS.process_message
                           );
      --
      END LOOP;

      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, 'Total Error Count: ' || TO_CHAR (l_count));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, CHR (12)); -- CHR(12) CREATES A MS WORD PAGE BREAK
      --
      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, '****************************** ');
      fnd_file.put_line (2, 'LOAN ISSUANCES ERRORS ');
      fnd_file.put_line (2, '****************************** ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            RPAD ('TAX_ID_NUMBER', 20, ' ')
                         || RPAD ('EMPLOYEE_NUMBER', 20, ' ')
                         || RPAD ('ERROR_MESSAGE', 100, ' ')
                        );
      fnd_file.put_line (2, RPAD ('*', 60, '*'));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      --
      l_count := 0;

      --
      FOR ERRORS IN (SELECT *
                       FROM xxarg_mercer_dc005_stage A
                      WHERE process_flag = 'E')
      LOOP
         --
         l_count := l_count + 1;
         fnd_file.put_line (2,
                               RPAD (ERRORS.employee_ssn, 20, ' ')
                            || RPAD (ERRORS.employee_number, 20, ' ')
                            || ERRORS.process_message
                           );
      --
      END LOOP;

      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, 'Total Error Count: ' || TO_CHAR (l_count));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, CHR (12)); -- CHR(12) CREATES A MS WORD PAGE BREAK
      --
      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, '****************************** ');
      fnd_file.put_line (2, 'INACTIVE LOAN ERRORS ');
      fnd_file.put_line (2, '****************************** ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2,
                            RPAD ('TAX_ID_NUMBER', 20, ' ')
                         || RPAD ('EMPLOYEE_NUMBER', 20, ' ')
                         || RPAD ('ERROR_MESSAGE', 100, ' ')
                        );
      fnd_file.put_line (2, RPAD ('*', 60, '*'));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      --
      l_count := 0;

      --
      FOR ERRORS IN (SELECT *
                       FROM xxarg_mercer_dc004_stage A
                      WHERE process_flag = 'E')
      LOOP
         --
         l_count := l_count + 1;
         fnd_file.put_line (2,
                               RPAD (ERRORS.employee_ssn, 20, ' ')
                            || RPAD (ERRORS.employee_number, 20, ' ')
                            || ERRORS.process_message
                           );
      --
      END LOOP;

      --
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, 'Total Error Count: ' || TO_CHAR (l_count));
      fnd_file.put_line (2, ' ');
      fnd_file.put_line (2, CHR (12)); -- CHR(12) CREATES A MS WORD PAGE BREAK
   --
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE g_end_program;
   END generate_dc_error_report;

--
--
   PROCEDURE process_hb_file (
      errbuf                   OUT      VARCHAR2,
      retcode                  OUT      NUMBER,
      p_business_group_id      IN       NUMBER,
      p_move_flag              IN       VARCHAR2,
      p_rerun_flag             IN       VARCHAR2,
      p_open_enrollment_flag   IN       VARCHAR2,
      p_debug_flag             IN       VARCHAR2
   )
   IS
--
      l_count                   NUMBER          := 0;
      l_success_count           NUMBER          := 0;
      l_error_count             NUMBER          := 0;
--
      l_full_name               VARCHAR2 (1000);
      l_person_id               NUMBER;
      l_assignment_id           NUMBER;
      l_assignment_number       VARCHAR2 (30);
      l_batch_id                NUMBER          := 0;
      l_batch_name              VARCHAR2 (30)   := NULL;
      l_object_version_number   NUMBER          := 0;
      l_batch_line_id           NUMBER          := 0;
--
      l_seq                     NUMBER          := 0;
      l_d_seq                   NUMBER          := 0;
      l_s_seq                   NUMBER          := 0;
      l_default_ded             VARCHAR2 (100);
      l_p_seq                   NUMBER          := 0;
      l_default_per             VARCHAR2 (100);
      l_default_sep             VARCHAR2 (100);
      l_ded_amt1                VARCHAR2 (100)  := NULL;
      l_ded_amt2                VARCHAR2 (100)  := NULL;
      l_ded_amt3                VARCHAR2 (100)  := NULL;
      l_ded_amt4                VARCHAR2 (100)  := NULL;
      l_ded_amt5                VARCHAR2 (100)  := NULL;
      l_ded_amt6                VARCHAR2 (100)  := NULL;
      l_ded_amt7                VARCHAR2 (100)  := NULL;
      l_ded_amt8                VARCHAR2 (100)  := NULL;
      l_ded_amt9                VARCHAR2 (100)  := NULL;
      l_ded_amt10               VARCHAR2 (100)  := NULL;
      l_ded_amt11               VARCHAR2 (100)  := NULL;
      l_ded_amt12               VARCHAR2 (100)  := NULL;
      l_ded_amt13               VARCHAR2 (100)  := NULL;
      l_ded_amt14               VARCHAR2 (100)  := NULL;
      l_ded_amt15               VARCHAR2 (100)  := NULL;
      l_erhsa_amt               NUMBER          := NULL;
      l_ded_amt                 NUMBER          := NULL;
      l_annual_ded_amt          NUMBER          := NULL;
      l_months                  NUMBER          := NULL;
      l_payroll_id              NUMBER          := NULL;
      l_check_date              DATE;
      l_effective_date          DATE;
      l_effective_year          NUMBER;
      hsa_retcode               NUMBER;
      hsa_errbuf                VARCHAR2 (1000);
      l_hsa_batch_id            NUMBER;
      l_business_group_id       NUMBER;
      l_element_type_id         NUMBER;
      l_element_link_id         NUMBER;
      v_count                   NUMBER          := 0;
      l_ele_flag                VARCHAR2 (1);
      l_element_entry_id        NUMBER          := 0;
      l_ovn                     NUMBER          := 0;
      l_eff_start_date          DATE            := NULL;
      l_eff_end_date            DATE            := NULL;
      l_warning                 BOOLEAN;
      l_status                  NUMBER;
      v_lak_count               NUMBER          := 0;
      v_laksha_output           VARCHAR2 (1000);

--
      CURSOR c_record IS
         SELECT tax_id_number, one_code, TO_DATE (as_of_date, 'MMDDYYYY')
                                                                         asd,
                TO_DATE (effective_date, 'MMDDYYYY') ed, employee_number,
                deduction_class, TRIM (deduction_code) dc, deduction_amount,
                REPLACE (REPLACE (annual_goal_amount, CHR (13), ''),
                         CHR (10),
                         ''
                        ) aga,
                SYSDATE, fnd_global.user_id UI
                , fnd_global.login_id, fnd_global.conc_program_id,
                fnd_global.conc_request_id
         FROM   xxarg_mercer_inbound_hb_file
          --WHERE ROWNUM < 10
          ;

--
      CURSOR src_recs
      IS
         SELECT     *
               FROM xxarg_mercer_inbound_hb_stage A
              WHERE 1 = 1
                AND NVL (A.process_flag, 'N') IN ('N', 'E')
                AND (   (p_open_enrollment_flag = 'Y'
                         AND deduction_amount <> 0
                        )
                     OR (p_open_enrollment_flag = 'N')
                    )
         --    AND EMPLOYEE_NUMBER='0560631   '
             --AND DEDUCTION_CODE='ERHSA'
         FOR UPDATE;

      CURSOR c1 (p_element_name VARCHAR2)
      IS
         SELECT   input_value_id, NAME, display_sequence,
                  petf.element_type_id                   --,PIVF.DEFAULT_VALUE
                                      ,
                  hr_general.decode_lookup (lookup_type,
                                            DEFAULT_VALUE
                                           ) DEFAULT_VALUE,
                  DEFAULT_VALUE per
             FROM pay_input_values_f pivf, pay_element_types_f petf
            WHERE pivf.element_type_id = petf.element_type_id
              AND TRUNC (SYSDATE) BETWEEN TRUNC (petf.effective_start_date)
                                      AND TRUNC (petf.effective_end_date)
              AND TRUNC (SYSDATE) BETWEEN TRUNC (pivf.effective_start_date)
                                      AND TRUNC (pivf.effective_end_date)
              AND petf.element_name = TRIM (p_element_name)
         ORDER BY display_sequence, NAME;
   BEGIN
      --
      g_debug_flag := p_debug_flag;
      --
      errbuf := NULL;
      retcode := 0;
      v_lak_count := 0;
      --
      fnd_file.put_line (1, ' ');
      fnd_file.put_line
                   (1,
                       'Mercer Health and Benefits Inbound process started: '
                    || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                   );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line
                  (debug_on,
                   'Inside package XXARG_MERCER_INBOUND_PKG.PROCESS_HB_FILE..'
                  );
      fnd_file.put_line (debug_on, 'Rerun Flag is ' || p_rerun_flag);
      --

      -- LAKSHA
       --
      fnd_file.put_line (fnd_file.LOG,
                         '====== Laksha Code -- Started ========= '
                        );
      fnd_file.put_line
                    (fnd_file.LOG,
                        'Mercer Health and Benefits Inbound process started: '
                     || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                    );
      fnd_file.put_line (fnd_file.LOG, ' ');
      fnd_file.put_line
                  (fnd_file.LOG,
                   'Inside package XXARG_MERCER_INBOUND_PKG.PROCESS_HB_FILE..'
                  );
      fnd_file.put_line (fnd_file.LOG,
                            'Rerun Flag is '
                         || p_rerun_flag
                         || '  :      Move Flag   =  '
                         || p_move_flag
                         || '  :      bus grp id =  '
                         || p_business_group_id
                         || '  :      open enrt flag =  '
                         || p_open_enrollment_flag
                         || '  :      dbug flag =  '
                         || p_debug_flag
                        );

      --

      --                    FOR C_REC_L IN C_RECORD
--                    LOOP
--                    V_LAK_COUNT := V_LAK_COUNT +1 ;
--                        V_LAKSHA_OUTPUT := C_REC_L.TAX_ID_NUMBER
--                        ||'  :  '||C_REC_L.ONE_CODE
--                        ||'  :  '||C_REC_L.ASD
--                        ||'  :  '||C_REC_L.ED
--                        ||'  :  '||C_REC_L.EMPLOYEE_NUMBER
--                        ||'  :  '||C_REC_L.DEDUCTION_CLASS
--                        ||'  :  '||C_REC_L.DC
--                        ||'  :  '||C_REC_L.DEDUCTION_AMOUNT
--                        ||'  :  '||C_REC_L.AGA
--                        ||'  :  '||C_REC_L.SYSDATE
--                        ||'  :  '||C_REC_L.UI
----                        ||'  :  '||C_REC_L.SYSDATE
----                        ||'  :  '||C_REC_L.FND_GLOBAL.USER_ID
----                        ||'  :  '||C_REC_L.FND_GLOBAL.LOGIN_ID
----                        ||'  :  '||C_REC_L.FND_GLOBAL.CONC_PROGRAM_ID
----                        ||'  :  '||C_REC_L.FND_GLOBAL.CONC_REQUEST_ID
--                             ;

      --                        FND_FILE.PUT_LINE(FND_FILE.LOG,'OUTPUT '||' :  ' ||V_LAK_COUNT||' :  ' ||V_LAKSHA_OUTPUT);
--
--                    END LOOP;

      -- LAKSHA

      --      --
      IF p_rerun_flag = 'N'
      THEN
         IF p_move_flag = 'Y'
         THEN
            BEGIN
               l_status := 0;
               l_status:= file_transfer('Mercer_HB.dat','Mercer_HB.dat');
               --L_STATUS := FILE_TRANSFER (P_FILE_NAME, P_FILE_NAME);

               IF l_status = 1
               THEN
                  errbuf := 'Error in the FTP Progrom ' || SQLERRM;
                  retcode := 2;
                  RAISE g_end_program;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  errbuf := 'Error in the FTP Progrom ' || SQLERRM;
                  retcode := 2;
                  RAISE g_end_program;
            END;
         END IF;

         --
         fnd_file.put_line (debug_on,
                            'cleanup the staging table after previous run'
                           );
         fnd_file.put_line
            (debug_on,
             'changing all un-processed recs from N to CN and error records from E to CE'
            );
         --

         --LAKSHA
         fnd_file.put_line (fnd_file.LOG,
                            'cleanup the staging table after previous run'
                           );
         fnd_file.put_line
            (fnd_file.LOG,
             'changing all un-processed recs from N to CN and error records from E to CE'
            );

         -- LAKSHA
         --
         BEGIN
            fnd_file.put_line (fnd_file.LOG,
                               'Before updating the stage Tab   '
                              );

            UPDATE xxarg_mercer_inbound_hb_stage
               SET process_flag = 'C' || NVL (process_flag, 'N')
             WHERE NVL (process_flag, 'N') IN ('N', 'E');

            --
            COMMIT;
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                     (1,
                      'Error updating the data stage table during clean up..'
                     );
               fnd_file.put_line
                  (fnd_file.LOG,
                   '-- Lak  --Error updating the data stage table during clean up..'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         fnd_file.put_line
                       (fnd_file.LOG,
                        'Lak -- now load the staging table from the data file'
                       );

         --
         BEGIN
            -- LAKSHA
            v_lak_count := v_lak_count + 1;
            fnd_file.put_line (fnd_file.LOG,
                               'Before inserting into staging table'
                              );
            fnd_file.put_line (fnd_file.LOG,
                               'Rec Count    =  ' || v_lak_count);

            -- LAKSHA
            INSERT INTO xxarg_mercer_inbound_hb_stage
                        (tax_id_number                                    -- 1
                                      ,
                         one_code                                         -- 2
                                 ,
                         as_of_date                                       -- 3
                                   ,
                         effective_date                                   -- 4
                                       ,
                         employee_number                                  -- 5
                                        ,
                         deduction_class                                  -- 6
                                        ,
                         deduction_code                                   -- 7
                                       ,
                         deduction_amount                                 -- 8
                                         ,
                         annual_goal_amount                               -- 9
                                           ,
                         creation_date                                   -- 10
                                      ,
                         created_by                                      -- 11
                                   ,
                         last_update_date                                -- 12
                                         ,
                         last_updated_by                                 -- 13
                                        ,
                         last_update_login                               -- 14
                                          ,
                         program_id                                      -- 15
                                   ,
                         request_id                                      -- 16
                                   ,
                         process_flag                                    -- 17
                                     ,
                         process_message                                 -- 18
                                        ,
                         pbh_batch_id                                    -- 19
                                     ,
                         pbl_batch_line_id                               -- 20
                                          )
               SELECT tax_id_number                                       -- 1
                                   ,
                      one_code                                            -- 2
                              ,
                      TO_DATE (as_of_date, 'MMDDYYYY')                    -- 3
                                                      ,
                      TO_DATE (effective_date, 'MMDDYYYY')                 --4
                                                          ,
                      employee_number                                     -- 5
                                     ,
                      deduction_class                                      --6
                                     ,
                      TRIM (deduction_code)                               -- 7
                                           ,
                      deduction_amount                                     --8
                                      ,
                      REPLACE (REPLACE (annual_goal_amount, CHR (13), ''),
                               CHR (10),
                               ''
                              )                                            --9
                               ,
                      SYSDATE                                            -- 10
                             ,
                      fnd_global.user_id                                 -- 11
                                        ,
                      SYSDATE                                            -- 12
                             ,
                      fnd_global.user_id                                 -- 13
                                        ,
                      fnd_global.login_id                                 --14
                                         ,
                      fnd_global.conc_program_id                         -- 15
                                                ,
                      fnd_global.conc_request_id                         -- 16
                                                ,
                      'N'                                                -- 17
                         ,
                      'Test '                               ---NULL       --18
                             ,
                      1                                         -- NULL  -- 19
                       ,
                      111                                   --NULL       -- 20
                 FROM xxarg_mercer_inbound_hb_file
                                                  --WHERE ROWNUM < 10000
            ;

            --
            COMMIT;
            fnd_file.put_line (fnd_file.LOG,
                               'Lak-Count of Records       ' || v_lak_count
                              );
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line
                       (fnd_file.LOG,
                           'Lak - Exception--Error inserting the stg table  '
                        || v_lak_count
                        || '   :   '
                        || SUBSTR (SQLERRM, 1, 220)
                       );
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                   (1,
                    'Error inserting data from source file into staging table'
                   );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;
      END IF;

      --
      --
      fnd_file.put_line (debug_on, 'Opening the detail data cursor');
      fnd_file.put_line (fnd_file.LOG, 'Lak - Before Opening the Cursor   ');

      --
      FOR cr IN src_recs
      LOOP
         fnd_file.put_line (fnd_file.LOG, 'Lak - in the Loop  ');
         --
         errbuf := NULL;
         retcode := 0;
         l_count := l_count + 1;
         l_ded_amt1 := NULL;
         l_ded_amt2 := NULL;
         l_ded_amt3 := NULL;
         l_ded_amt4 := NULL;
         l_ded_amt5 := NULL;
         l_ded_amt6 := NULL;
         l_ded_amt7 := NULL;
         l_ded_amt8 := NULL;
         l_ded_amt9 := NULL;
         l_ded_amt10 := NULL;
         l_ded_amt11 := NULL;
         l_ded_amt12 := NULL;
         l_ded_amt13 := NULL;
         l_ded_amt14 := NULL;
         l_ded_amt15 := NULL;
         l_erhsa_amt := NULL;
         l_ded_amt := NULL;
         l_annual_ded_amt := NULL;
         l_months := NULL;
         l_payroll_id := NULL;
         l_check_date := NULL;
         l_effective_date := NULL;
         l_effective_year := NULL;
         l_business_group_id := NULL;
         l_element_type_id := NULL;
         l_element_link_id := NULL;
         l_element_entry_id := NULL;
         l_ovn := NULL;
         l_batch_line_id := 0;

         --
         IF l_count = 1
         THEN
            --
            fnd_file.put_line (debug_on, 'Creating Pay Batch Header..');
            --
            fnd_file.put_line (fnd_file.LOG,
                               'Lak -Creating the Batch header '
                              );

            --
            BEGIN
               SELECT xxarg_hr_hsa_batch_id_s.NEXTVAL
                 INTO l_hsa_batch_id
                 FROM SYS.DUAL;

               fnd_file.put_line (fnd_file.LOG,
                                  'Lak - Batch id ' || l_hsa_batch_id
                                 );
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, 'Error while getting sequence HSA');
            END;

            BEGIN
               --
               l_batch_name :=
                          'MERCER_HB_' || TO_CHAR (SYSDATE, 'YYYYMMDD_SSSSS');
               fnd_file.put_line (fnd_file.LOG,
                                  'Lak - Batch Name   ' || l_batch_name
                                 );
               --
               pay_batch_element_entry_api.create_batch_header
                    (p_session_date                => TRUNC (cr.effective_date),
                     p_batch_name                  => l_batch_name,
                     p_business_group_id           => p_business_group_id,
                     p_action_if_exists            => 'U',
                     p_batch_source                => 'Mercer HB File',
                     p_batch_reference             => fnd_date.date_to_canonical
                                                                      (SYSDATE),
                     p_date_effective_changes      => 'U',
                     p_batch_id                    => l_batch_id,
                     p_object_version_number       => l_object_version_number
                    );
               --
               fnd_file.put_line (debug_on,
                                     'Batch ID created successfully: '
                                  || TO_CHAR (l_batch_id)
                                 );
               fnd_file.put_line (debug_on, 'Batch Name: ' || l_batch_name);
               fnd_file.put_line (debug_on, ' ');
               fnd_file.put_line (fnd_file.LOG,
                                  'Lak - Batch created succcesssssfulllllly'
                                 );
               fnd_file.put_line (fnd_file.LOG,
                                     'Lak -Batch ID created successfully: '
                                  || TO_CHAR (l_batch_id)
                                 );
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
                  fnd_file.put_line (1, 'Error creating Pay Batch Header');
                  errbuf := 'Error creating Pay Batch Header ' || SQLERRM;
                  RAISE g_end_program;
            END;
         --
         END IF;

         --
         fnd_file.put_line (debug_on, ' ');
         fnd_file.put_line (debug_on, 'Tax ID#: ' || cr.tax_id_number);
         fnd_file.put_line (debug_on, 'Employee#: ' || cr.employee_number);
         fnd_file.put_line (debug_on,
                               'Effective Date: '
                            || TO_CHAR (cr.effective_date, 'DD-MON-RRRR')
                           );
         fnd_file.put_line (debug_on, 'Deduction Code: ' || cr.deduction_code);
         fnd_file.put_line (debug_on,
                               'Deduction Amount: '
                            || TO_CHAR (cr.deduction_amount)
                           );
         fnd_file.put_line (debug_on, 'Fetching the employee details..');

         --
         BEGIN
            --
            SELECT DISTINCT papf.full_name, papf.person_id,
                            paaf.assignment_id, paaf.assignment_number,
                            paaf.payroll_id, paaf.business_group_id
                       INTO l_full_name, l_person_id,
                            l_assignment_id, l_assignment_number,
                            l_payroll_id, l_business_group_id
                       FROM per_all_people_f papf, per_all_assignments_f paaf
                      WHERE LTRIM (papf.employee_number, 0) =
                                          LTRIM (TRIM (cr.employee_number), 0)
                        AND xxarg_unfrmt_natl_id_fnc (papf.national_identifier) =
                                   xxarg_unfrmt_natl_id_fnc (cr.tax_id_number)
                        AND paaf.person_id = papf.person_id
                        AND TRUNC (cr.effective_date)
                               BETWEEN TRUNC (papf.effective_start_date)
                                   AND TRUNC (papf.effective_end_date)
                        AND TRUNC (cr.effective_date)
                               BETWEEN TRUNC (paaf.effective_start_date)
                                   AND TRUNC (paaf.effective_end_date)
                        AND paaf.primary_flag = 'Y';

            --
            fnd_file.put_line (debug_on, 'Employee Name: ' || l_full_name);
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                          (1,
                              'Error fetching the employee details for SSN  '
                           || xxarg_unfrmt_natl_id_fnc (cr.tax_id_number)
                          );
               errbuf := 'Error fetching the employee details  ' || SQLERRM;
               retcode := 1;
               GOTO end_of_loop;
         END;

         IF cr.deduction_amount = 0
         THEN
            BEGIN
               SELECT peef.element_entry_id, peef.object_version_number
                 INTO l_element_entry_id, l_ovn
                 FROM per_all_people_f papf,
                      per_all_assignments_f paaf,
                      pay_element_entries_f peef,
                      pay_element_links_f pelf,
                      pay_element_types_f petf
                WHERE papf.person_id = paaf.person_id
                  AND paaf.assignment_id = peef.assignment_id
                  AND peef.element_link_id = pelf.element_link_id
                  AND pelf.element_type_id = petf.element_type_id
                  AND peef.entry_type = 'E'
                  AND UPPER (petf.element_name) LIKE
                                              UPPER (TRIM (cr.deduction_code))
                  AND paaf.assignment_id = l_assignment_id
                  AND peef.effective_end_date <> cr.effective_date
                  AND cr.effective_date BETWEEN papf.effective_start_date
                                            AND papf.effective_end_date
                  AND cr.effective_date BETWEEN paaf.effective_start_date
                                            AND paaf.effective_end_date
                  AND cr.effective_date BETWEEN peef.effective_start_date
                                            AND peef.effective_end_date
                  AND cr.effective_date BETWEEN pelf.effective_start_date
                                            AND pelf.effective_end_date
                  AND cr.effective_date BETWEEN petf.effective_start_date
                                            AND petf.effective_end_date;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  retcode := 0;
                  GOTO end_of_loop;
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                             (1,
                              'ERROR! Fetching the element entry details... '
                             );
                  fnd_file.put_line (1, 'Emp#: ' || TRIM (cr.employee_number));
                  errbuf :=
                      'ERROR! Fetching the element entry details. ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop;
            END;

            --
            --
            fnd_file.put_line
                         (debug_on,
                          'now calling the API to delete the element entry.. '
                         );

            --
            BEGIN
               pay_element_entry_api.delete_element_entry
                                 (p_datetrack_delete_mode      => 'DELETE',
                                  p_effective_date             => cr.effective_date,
                                  p_element_entry_id           => l_element_entry_id,
                                  p_object_version_number      => l_ovn,
                                  p_effective_start_date       => l_eff_start_date,
                                  p_effective_end_date         => l_eff_end_date,
                                  p_delete_warning             => l_warning
                                 );
               l_batch_line_id := 1;
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                             (debug_on,
                              'ERROR! calling the delete_element_entry API..'
                             );
                  fnd_file.put_line (debug_on, 'ERRMSG: ' || SQLERRM);
                  errbuf :=
                     'ERROR! calling the delete_element_entry API..'
                     || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop;
            END;
         ELSE
            --
            --
            fnd_file.put_line (debug_on,
                               'Fetch the Element Entry Value details..'
                              );

                      --
            --- FOR ERHSA ELEMENT POPULATE MAXIMUM AMOUNT INPUT VALUE ALSO
            IF (   TRIM (cr.deduction_code) = 'ERHSA'
                OR TRIM (cr.deduction_code) = 'ERHSA1'
               )
            THEN
               BEGIN
                  /*  SELECT        PIVF.DISPLAY_SEQUENCE
                    INTO          L_SEQ
                    FROM          PAY_INPUT_VALUES_F PIVF
                                  ,PAY_ELEMENT_TYPES_F PETF
                    WHERE         PIVF.ELEMENT_TYPE_ID = PETF.ELEMENT_TYPE_ID
                          AND     TRUNC(SYSDATE)  BETWEEN TRUNC(PETF.EFFECTIVE_START_DATE)
                                                  AND     TRUNC(PETF.EFFECTIVE_END_DATE)
                          AND     TRUNC(SYSDATE)  BETWEEN TRUNC(PIVF.EFFECTIVE_START_DATE)
                                                  AND     TRUNC(PIVF.EFFECTIVE_END_DATE)
                          AND     PETF.ELEMENT_NAME = TRIM(CR.DEDUCTION_CODE)
                          AND     UPPER(PIVF.NAME) =  'MAXIMUM AMOUNT';*/
                  v_count := 0;
                  l_seq := 0;

                  FOR c1_rec IN c1 ((cr.deduction_code))
                  LOOP
                     v_count := v_count + 1;

                     IF (UPPER (c1_rec.NAME) = 'MAXIMUM AMOUNT')
                     THEN
                        l_seq := v_count;
                     END IF;
                  END LOOP;

                  IF l_seq = 0
                  THEN
                     fnd_file.put_line (1, ' ');
                     fnd_file.put_line
                        (1,
                            'Error fetching the Element input value Maximum Amount for element '
                         || TRIM (cr.deduction_code)
                        );
                     errbuf :=
                           'Error fetching the Element input value Maximum Amount for element '
                        || SQLERRM;
                     retcode := 1;
                     GOTO end_of_loop;
                  END IF;

                  --
                  fnd_file.put_line
                     (debug_on,
                         'Input Value " Max Amount" has a display sequence of: '
                      || TO_CHAR (l_seq)
                     );
                  fnd_file.put_line
                             (debug_on,
                                 'Hence pay_value_'
                              || TO_CHAR (l_seq)
                              || ' will be used to populate the deduction amount'
                             );
                  --
                  l_annual_ded_amt := cr.annual_goal_amount;

                  IF l_seq = 1
                  THEN
                     l_ded_amt1 := l_annual_ded_amt;
                  ELSIF l_seq = 2
                  THEN
                     l_ded_amt2 := l_annual_ded_amt;
                  ELSIF l_seq = 3
                  THEN
                     l_ded_amt3 := l_annual_ded_amt;
                  ELSIF l_seq = 4
                  THEN
                     l_ded_amt4 := l_annual_ded_amt;
                  ELSIF l_seq = 5
                  THEN
                     l_ded_amt5 := l_annual_ded_amt;
                  ELSIF l_seq = 6
                  THEN
                     l_ded_amt6 := l_annual_ded_amt;
                  ELSIF l_seq = 7
                  THEN
                     l_ded_amt7 := l_annual_ded_amt;
                  ELSIF l_seq = 8
                  THEN
                     l_ded_amt8 := l_annual_ded_amt;
                  ELSIF l_seq = 9
                  THEN
                     l_ded_amt9 := l_annual_ded_amt;
                  ELSIF l_seq = 10
                  THEN
                     l_ded_amt10 := l_annual_ded_amt;
                  ELSIF l_seq = 11
                  THEN
                     l_ded_amt11 := l_annual_ded_amt;
                  ELSIF l_seq = 12
                  THEN
                     l_ded_amt12 := l_annual_ded_amt;
                  ELSIF l_seq = 13
                  THEN
                     l_ded_amt13 := l_annual_ded_amt;
                  ELSIF l_seq = 14
                  THEN
                     l_ded_amt14 := l_annual_ded_amt;
                  ELSIF l_seq = 15
                  THEN
                     l_ded_amt15 := l_annual_ded_amt;
                  END IF;
               --
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     fnd_file.put_line (1, ' ');
                     fnd_file.put_line
                        (1,
                            'Error fetching the Element input value Maximum Amount for element '
                         || TRIM (cr.deduction_code)
                        );
                     errbuf :=
                           'Error fetching the Element input value Maximum Amount for element '
                        || SQLERRM;
                     retcode := 1;
                     GOTO end_of_loop;
               END;
            END IF;

            fnd_file.put_line (debug_on,
                               'Fetch the Element Entry Value details.'
                              );

            BEGIN
               /*   SELECT        PIVF.DISPLAY_SEQUENCE ,PETF.ELEMENT_TYPE_ID
                  INTO          L_SEQ,L_ELEMENT_TYPE_ID
                  FROM          PAY_INPUT_VALUES_F PIVF
                                ,PAY_ELEMENT_TYPES_F PETF
                  WHERE         PIVF.ELEMENT_TYPE_ID = PETF.ELEMENT_TYPE_ID
                        AND     TRUNC(SYSDATE)  BETWEEN TRUNC(PETF.EFFECTIVE_START_DATE)
                                                AND     TRUNC(PETF.EFFECTIVE_END_DATE)
                        AND     TRUNC(SYSDATE)  BETWEEN TRUNC(PIVF.EFFECTIVE_START_DATE)
                                                AND     TRUNC(PIVF.EFFECTIVE_END_DATE)
                        AND     PETF.ELEMENT_NAME = TRIM(CR.DEDUCTION_CODE)
                        AND     UPPER(PIVF.NAME) = 'AMOUNT';*/
               v_count := 0;
               l_seq := 0;
               l_d_seq := 0;
               l_s_seq := 0;
               l_default_ded := NULL;
               l_default_sep := NULL;
               l_p_seq := 0;
               l_default_per := NULL;

--                        FND_FILE.PUT_LINE(DEBUG_ON,'FETCH THE ELEMENT ENTRY VALUE DETAILS1.');
               FOR c1_rec IN c1 ((cr.deduction_code))
               LOOP
--FND_FILE.PUT_LINE(DEBUG_ON,'FETCH THE ELEMENT ENTRY VALUE DETAILS2.');
                  v_count := v_count + 1;
                  l_element_type_id := c1_rec.element_type_id;

                  IF (UPPER (c1_rec.NAME) = 'AMOUNT')
                  THEN
                     l_seq := v_count;
                  END IF;

                  IF (UPPER (c1_rec.NAME) = 'DEDUCTION PROCESSING')
                  THEN
                     l_d_seq := v_count;
                     l_default_ded := c1_rec.DEFAULT_VALUE;
                  END IF;

                  IF (UPPER (c1_rec.NAME) = 'SEPARATE CHECK')
                  THEN
                     l_s_seq := v_count;
                     l_default_sep := c1_rec.DEFAULT_VALUE;
                  END IF;

                  IF (   TRIM (cr.deduction_code) = 'HIMEDA'
                      OR TRIM (cr.deduction_code) = 'HIMEDB'
                     )
                  THEN
                     IF (UPPER (c1_rec.NAME) = 'PERCENTAGE')
                     THEN
                        l_p_seq := v_count;
                        l_default_per := c1_rec.per;
                     END IF;
                  END IF;
               END LOOP;

               IF (   TRIM (cr.deduction_code) = 'HIMEDA'
                   OR TRIM (cr.deduction_code) = 'HIMEDB'
                  )
               THEN
                  NULL;
               ELSE
                  IF l_seq = 0
                  THEN
                     fnd_file.put_line (1, ' ');
                     fnd_file.put_line
                        (1,
                            'Error fetching the Element input value AMOUNT for element '
                         || TRIM (cr.deduction_code)
                        );
                     errbuf :=
                           'Error fetching the Element input value  AMOUNT for element '
                        || SQLERRM;
                     retcode := 1;
                     GOTO end_of_loop;
                  END IF;
               END IF;

--FND_FILE.PUT_LINE(DEBUG_ON,'FETCH THE ELEMENT ENTRY VALUE DETAILS3.');

               --
               fnd_file.put_line
                        (debug_on,
                            'Input Value "Amount" has a display sequence of: '
                         || TO_CHAR (l_seq)
                        );
               fnd_file.put_line
                             (debug_on,
                                 'Hence pay_value_'
                              || TO_CHAR (l_seq)
                              || ' will be used to populate the deduction amount'
                             );

               ---CHECKING FOR TESTING PURPOSE
               BEGIN
                  l_element_link_id :=
                     hr_entry_api.get_link (l_assignment_id,
                                            l_element_type_id,
                                            cr.effective_date
                                           );

                  IF l_element_link_id IS NULL
                  THEN
                     fnd_file.put_line (1, ' ');
                     fnd_file.put_line
                                  (1,
                                      'Element Link not found for element '
                                   || TRIM (cr.deduction_code)
                                   || ' SSN '
                                   || xxarg_unfrmt_natl_id_fnc
                                                             (cr.tax_id_number)
                                  );
                     errbuf :=
                              'Element Link not found for element ' || SQLERRM;
                     retcode := 1;
                     GOTO end_of_loop;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     fnd_file.put_line (1, ' ');
                     fnd_file.put_line
                                  (1,
                                      'Element Link not found for element '
                                   || TRIM (cr.deduction_code)
                                   || ' SSN '
                                   || xxarg_unfrmt_natl_id_fnc
                                                             (cr.tax_id_number)
                                  );
                     errbuf :=
                              'Element Link not found for element ' || SQLERRM;
                     retcode := 1;
                     GOTO end_of_loop;
               END;

                 --
               --- FOR ERHSA ELEMENT ,
               ----1. NEED TO POPULATE MONTHLY DEDUCTION
               -----2. THE ELEMENT WILL START IN DEC AND END IN NOV FOLLOWING YEAR
               ------3. THIS ELEMENT WILL DEDUCT IN FIRST PAY PERIOD OF THE MONTH, SO CHECK WITH
                     -- FIRST EFFECTIVE DATE OF THE MONTH, IF GREATER THEN
               IF (   TRIM (cr.deduction_code) = 'ERHSA'
                   OR TRIM (cr.deduction_code) = 'ERHSA1'
                  )
               THEN
                  BEGIN
                     SELECT MIN (end_date)
                       INTO l_check_date
                       FROM per_time_periods P
                      WHERE payroll_id = l_payroll_id
                        AND TO_CHAR (end_date, 'MON-YY') =
                                         TO_CHAR (cr.effective_date, 'MON-YY');
                  END;

                  l_effective_date := cr.effective_date;

                  IF (l_effective_date <
                                 '30-NOV-' || TO_CHAR (l_effective_date, 'YY')
                     )
                  THEN
                     l_effective_year := TO_CHAR (l_effective_date, 'YY');
                  ELSE
                     l_effective_year := TO_CHAR (l_effective_date, 'YY') + 1;
                  END IF;

                  IF l_check_date >= l_effective_date
                  THEN
                     l_effective_date := TRUNC (cr.effective_date, 'MM');
                  ELSE
                     l_effective_date :=
                              ADD_MONTHS (TRUNC (cr.effective_date, 'MM'), 1);
                  END IF;

                  --SELECT 'Y' INTO L_EMP_CHECK
                  -- FROM XXARG_HR_HSA_DETAILS WHERE HSA_BATCH_ID=0
                  l_ded_amt := cr.annual_goal_amount;
                  l_months :=
                     CEIL (MONTHS_BETWEEN ('30-NOV-' || l_effective_year,
                                           l_effective_date
                                          )
                          );

                  IF p_open_enrollment_flag = 'Y'
                  THEN
                     l_erhsa_amt := CEIL (l_ded_amt * 100 / 12) / 100;

                     IF l_erhsa_amt <> 0
                     THEN
                        BEGIN
                           INSERT INTO xxarg.xxarg_hr_hsa_details
                                       (hsa_batch_id,
--  USE XXARG_HR_HSA_BATCH_ID_S.NEXTVAL AND USE THE SAME VALUE FOR ALL THE ROWS
                                        payroll_employee_id,
                        -- XXARG_UNFRMT_NATL_ID_FNC (PAPF.NATIONAL_IDENTIFIER)
                                        funding_source,
                             -- XXARG_HR_HSA_PKG.GET_TRANSLATED_VAL_FOR_ELEM (
                                                       --    PI_ELEMENT_NAME        IN PAY_ELEMENT_TYPES_F.ELEMENT_NAME%TYPE,
                                                         --  PI_BUSINESS_GROUP_ID   IN PAY_ELEMENT_TYPES_F.BUSINESS_GROUP_ID%TYPE
                                                           --  )
                                                       transaction_type,
                                             -- XXARG_HR_HSA_PKG.GC_HSA_CREDIT
                                        amount,
                  -- AMOUNT IN DOLLARS, USUALLY CEIL(YEARLY_AMOUNT*100/12)/100
                                        description,
                                          -- 'OPEN ENROLLMENT JANUARY FUNDING'
                                        effective_date,
                                          --  '01-JAN-YYYY' (CURRENT YEAR + 1)
                                                       payroll_action_id, -- 0
                                        element_name,
                                                 -- ELEMENT_NAME USUALLY ERHSA
                                        business_group_id,
                                               -- BUSINESS_GROUP_ID, USUALLY 0
                                        internal_description,
                        -- 'MERCER INBOUND INTERFACE OPEN ENROLLEMENT SPECIAL'
                                        created_by,      -- FND_GLOBAL.USER_ID
                                                   creation_date,   -- SYSDATE
                                        last_updated_by, -- FND_GLOBAL.USER_ID
                                        last_update_date            -- SYSDATE
                                       )
                                VALUES (l_hsa_batch_id,
                                        xxarg_unfrmt_natl_id_fnc
                                                             (cr.tax_id_number),
                                        --  APPS.XXARG_HR_HSA_PKG.GET_TRANSLATED_VAL_FOR_ELEM (
                                          --                                                TRIM(CR.DEDUCTION_CODE)--IN PAY_ELEMENT_TYPES_F.ELEMENT_NAME%TYPE,
                                            --                                             , L_BUSINESS_GROUP_ID--IN PAY_ELEMENT_TYPES_F.BUSINESS_GROUP_ID%TYPE
                                              --                                             ),
                                        2, 'CR',
                                        l_erhsa_amt,
                                        'Open Enrollment January Funding',
                                        '01-JAN-' || l_effective_year, 0,
                                        TRIM (cr.deduction_code),
                                        l_business_group_id,
                                        'Mercer Inbound Interface Open Enrollement Special',
                                        fnd_global.user_id, SYSDATE,
                                        fnd_global.user_id,
                                        SYSDATE
                                       );
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              fnd_file.put_line
                                 (1,
                                     'ERROR while loading ERHSA details into the XXARG.XXARG_HR_HSA_DETAILS for Employee '
                                  || l_assignment_number
                                 );
                        END;
                     END IF;
                  ELSE
                     l_erhsa_amt := CEIL (l_ded_amt * 100 / l_months) / 100;
                  END IF;

                  l_ded_amt := l_erhsa_amt;
               ELSE
                  l_ded_amt := cr.deduction_amount;
               END IF;

               IF l_seq = 1
               THEN
                  l_ded_amt1 := l_ded_amt;
               ELSIF l_seq = 2
               THEN
                  l_ded_amt2 := l_ded_amt;
               ELSIF l_seq = 3
               THEN
                  l_ded_amt3 := l_ded_amt;
               ELSIF l_seq = 4
               THEN
                  l_ded_amt4 := l_ded_amt;
               ELSIF l_seq = 5
               THEN
                  l_ded_amt5 := l_ded_amt;
               ELSIF l_seq = 6
               THEN
                  l_ded_amt6 := l_ded_amt;
               ELSIF l_seq = 7
               THEN
                  l_ded_amt7 := l_ded_amt;
               ELSIF l_seq = 8
               THEN
                  l_ded_amt8 := l_ded_amt;
               ELSIF l_seq = 9
               THEN
                  l_ded_amt9 := l_ded_amt;
               ELSIF l_seq = 10
               THEN
                  l_ded_amt10 := l_ded_amt;
               ELSIF l_seq = 11
               THEN
                  l_ded_amt11 := l_ded_amt;
               ELSIF l_seq = 12
               THEN
                  l_ded_amt12 := l_ded_amt;
               ELSIF l_seq = 13
               THEN
                  l_ded_amt13 := l_ded_amt;
               ELSIF l_seq = 14
               THEN
                  l_ded_amt14 := l_ded_amt;
               ELSIF l_seq = 15
               THEN
                  l_ded_amt15 := l_ded_amt;
               END IF;

               IF l_d_seq <> 0
               THEN
                  IF l_d_seq = 1
                  THEN
                     l_ded_amt1 := l_default_ded;
                  ELSIF l_d_seq = 2
                  THEN
                     l_ded_amt2 := l_default_ded;
                  ELSIF l_d_seq = 3
                  THEN
                     l_ded_amt3 := l_default_ded;
                  ELSIF l_d_seq = 4
                  THEN
                     l_ded_amt4 := l_default_ded;
                  ELSIF l_d_seq = 5
                  THEN
                     l_ded_amt5 := l_default_ded;
                  ELSIF l_d_seq = 6
                  THEN
                     l_ded_amt6 := l_default_ded;
                  ELSIF l_d_seq = 7
                  THEN
                     l_ded_amt7 := l_default_ded;
                  ELSIF l_d_seq = 8
                  THEN
                     l_ded_amt8 := l_default_ded;
                  ELSIF l_d_seq = 9
                  THEN
                     l_ded_amt9 := l_default_ded;
                  ELSIF l_d_seq = 10
                  THEN
                     l_ded_amt10 := l_default_ded;
                  ELSIF l_d_seq = 11
                  THEN
                     l_ded_amt11 := l_default_ded;
                  ELSIF l_d_seq = 12
                  THEN
                     l_ded_amt12 := l_default_ded;
                  ELSIF l_d_seq = 13
                  THEN
                     l_ded_amt13 := l_default_ded;
                  ELSIF l_d_seq = 14
                  THEN
                     l_ded_amt14 := l_default_ded;
                  ELSIF l_d_seq = 15
                  THEN
                     l_ded_amt15 := l_default_ded;
                  END IF;
               END IF;

               IF l_s_seq <> 0
               THEN
                  IF l_s_seq = 1
                  THEN
                     l_ded_amt1 := l_default_sep;
                  ELSIF l_s_seq = 2
                  THEN
                     l_ded_amt2 := l_default_sep;
                  ELSIF l_s_seq = 3
                  THEN
                     l_ded_amt3 := l_default_sep;
                  ELSIF l_s_seq = 4
                  THEN
                     l_ded_amt4 := l_default_sep;
                  ELSIF l_s_seq = 5
                  THEN
                     l_ded_amt5 := l_default_sep;
                  ELSIF l_s_seq = 6
                  THEN
                     l_ded_amt6 := l_default_sep;
                  ELSIF l_s_seq = 7
                  THEN
                     l_ded_amt7 := l_default_sep;
                  ELSIF l_s_seq = 8
                  THEN
                     l_ded_amt8 := l_default_sep;
                  ELSIF l_s_seq = 9
                  THEN
                     l_ded_amt9 := l_default_sep;
                  ELSIF l_s_seq = 10
                  THEN
                     l_ded_amt10 := l_default_sep;
                  ELSIF l_s_seq = 11
                  THEN
                     l_ded_amt11 := l_default_sep;
                  ELSIF l_s_seq = 12
                  THEN
                     l_ded_amt12 := l_default_sep;
                  ELSIF l_s_seq = 13
                  THEN
                     l_ded_amt13 := l_default_sep;
                  ELSIF l_s_seq = 14
                  THEN
                     l_ded_amt14 := l_default_sep;
                  ELSIF l_s_seq = 15
                  THEN
                     l_ded_amt15 := l_default_sep;
                  END IF;
               END IF;

               IF l_p_seq <> 0
               THEN
                  IF l_p_seq = 1
                  THEN
                     l_ded_amt1 := l_default_per;
                  ELSIF l_p_seq = 2
                  THEN
                     l_ded_amt2 := l_default_per;
                  ELSIF l_p_seq = 3
                  THEN
                     l_ded_amt3 := l_default_per;
                  ELSIF l_p_seq = 4
                  THEN
                     l_ded_amt4 := l_default_per;
                  ELSIF l_p_seq = 5
                  THEN
                     l_ded_amt5 := l_default_per;
                  ELSIF l_p_seq = 6
                  THEN
                     l_ded_amt6 := l_default_per;
                  ELSIF l_p_seq = 7
                  THEN
                     l_ded_amt7 := l_default_per;
                  ELSIF l_p_seq = 8
                  THEN
                     l_ded_amt8 := l_default_per;
                  ELSIF l_p_seq = 9
                  THEN
                     l_ded_amt9 := l_default_per;
                  ELSIF l_p_seq = 10
                  THEN
                     l_ded_amt10 := l_default_per;
                  ELSIF l_p_seq = 11
                  THEN
                     l_ded_amt11 := l_default_per;
                  ELSIF l_p_seq = 12
                  THEN
                     l_ded_amt12 := l_default_per;
                  ELSIF l_p_seq = 13
                  THEN
                     l_ded_amt13 := l_default_per;
                  ELSIF l_p_seq = 14
                  THEN
                     l_ded_amt14 := l_default_per;
                  ELSIF l_p_seq = 15
                  THEN
                     l_ded_amt15 := l_default_per;
                  END IF;
               END IF;
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
                  fnd_file.put_line
                     (1,
                         'Error fetching the Element input value AMOUNT for element '
                      || TRIM (cr.deduction_code)
                     );
                  errbuf :=
                        'Error fetching the Element input value  AMOUNT for element '
                     || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop;
            END;

            --
            --
            fnd_file.put_line (debug_on, 'Create the Pay Batch Line');

--              RAM
    --          FND_FILE.PUT_LINE(1,L_DEFAULT_SEP||' '||L_DEFAULT_DED);
              --
            BEGIN
               --
               pay_batch_element_entry_api.create_batch_line
                  (p_session_date               => SYSDATE,
                   p_batch_id                   => l_batch_id,
                   p_assignment_id              => l_assignment_id,
                   p_assignment_number          => l_assignment_number
                                                                      -- ,P_DATE_EARNED            =>    TO_DATE(CR.EFFECTIVE_DATE,'YYYYMMDD')
               ,
                   p_effective_date             => cr.effective_date
                                       --TO_DATE(CR.EFFECTIVE_DATE,'YYYYMMDD')
                                                                    ,
                   p_element_name               => cr.deduction_code,
                   p_entry_type                 => 'E'
                                            -- THIS IS A REGULAR ELEMENT ENTRY
                                                      ,
                   p_value_1                    => l_ded_amt1,
                   p_value_2                    => l_ded_amt2,
                   p_value_3                    => l_ded_amt3,
                   p_value_4                    => l_ded_amt4,
                   p_value_5                    => l_ded_amt5,
                   p_value_6                    => l_ded_amt6,
                   p_value_7                    => l_ded_amt7,
                   p_value_8                    => l_ded_amt8,
                   p_value_9                    => l_ded_amt9,
                   p_value_10                   => l_ded_amt10,
                   p_value_11                   => l_ded_amt11,
                   p_value_12                   => l_ded_amt12,
                   p_value_13                   => l_ded_amt13,
                   p_value_14                   => l_ded_amt14,
                   p_value_15                   => l_ded_amt15
                                                                         --             ,P_CANONICAL_DATE_FORMAT        =>'N'-- IN     VARCHAR2 DEFAULT 'Y'
                                                              -- ,P_IV_ALL_INTERNAL_FORMAT      =>'Y'--  IN     VARCHAR2 DEFAULT 'N'
               ,
                   p_batch_line_id              => l_batch_line_id,
                   p_object_version_number      => l_object_version_number
                  );
               --
               fnd_file.put_line (debug_on,
                                     'Batch Line created successfully: '
                                  || TO_CHAR (l_batch_line_id)
                                 );
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
--RAM
        --                 FND_FILE.PUT_LINE(1,SQLERRM);
                  fnd_file.put_line (1,
                                     'Error creating the Batch Line'
                                     || SQLERRM
                                    );
                  errbuf := 'Error creating the Batch Line ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop;
            END;
         END IF;                                            --DEDUCTION AMOUNT

                      --
         --
         <<end_of_loop>>
         BEGIN
            IF retcode = 0
            THEN
               --
               fnd_file.put_line (debug_on,
                                  'Updating the source record with success'
                                 );
               l_success_count := l_success_count + 1;

               --
               UPDATE xxarg_mercer_inbound_hb_stage
                  SET process_flag = 'Y',
                      process_message = NULL,
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = l_batch_id,
                      pbl_batch_line_id = l_batch_line_id
                WHERE CURRENT OF src_recs;
            ELSE
               l_error_count := l_error_count + 1;

               UPDATE xxarg_mercer_inbound_hb_stage
                  SET process_flag = 'E',
                      process_message = SUBSTR(errbuf, 1, 250), -- only post the first 250 characters of error message (CDCummins, 27-MAY-2011)
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = NULL,
                      pbl_batch_line_id = NULL
                WHERE CURRENT OF src_recs;
            END IF;
         END;
      END LOOP;

      BEGIN
         IF p_open_enrollment_flag = 'Y'
         THEN
            fnd_file.put_line (2, 'HSA Batch ID created: ' || l_hsa_batch_id);
            fnd_file.put_line
               (2,
                '     Run this concurrent program:  XXARG HR HSA Special Balance Adjust '
               );
--               FND_FILE.PUT_LINE(DEBUG_ON,'CALLING HR HSA PKG');
    --          XXARG_HR_HSA_PKG.ADD_EMP_BYPASS_PAYROLL(HSA_ERRBUF,HSA_RETCODE,L_REC_VAL);
        /*
              IF HSA_RETCODE <>  XXARG_HR_HSA_PKG.GN_RETCODE_SUCCESS THEN
              FND_FILE.PUT_LINE(2,' ');
    FND_FILE.PUT_LINE(2,' ');
    FND_FILE.PUT_LINE(2,'                    ARG-MERCER HB INBOUND ERHSA BALANCE ADJUSTMENT  ERROR REPORT');
    FND_FILE.PUT_LINE(2,'                    ************************************************************');
    FND_FILE.PUT_LINE(2,' ');
    FND_FILE.PUT_LINE(2,'DATE/TIME: '||TO_CHAR(SYSDATE,'DD-MON-RRRR HH24:MI:SS'));
    FND_FILE.PUT_LINE(2,' ');
    FND_FILE.PUT_LINE(2,' ');
    FND_FILE.PUT_LINE(2,   RPAD('TAX_ID_NUMBER',20,' ')
                        -- ||RPAD('EMPLOYEE_NUMBER',20,' ')
                         ||RPAD('DEDUCTION_CODE',20,' ')
                         ||LPAD('DEDUCTION_AMOUNT',20,' ')
                         ||RPAD('     ERROR_MESSAGE',100,' ')
                      );
    FND_FILE.PUT_LINE(2,RPAD('*',100,'*'));
    FND_FILE.PUT_LINE(2,' ');
    FND_FILE.PUT_LINE(2,' ');

              FOR I IN 1 .. L_REC_VAL.COUNT LOOP
              IF L_REC_VAL(I).INTERNAL_DESCRIPTION IS NOT NULL THEN
               FND_FILE.PUT_LINE(2,   RPAD(L_REC_VAL(I).PAYROLL_EMPLOYEE_ID,20,' ')
                         --||RPAD(I.EMPLOYEE_NUMBER,20,' ')
                         ||RPAD(L_REC_VAL(I).ELEMENT_NAME,20,' ')
                         ||LPAD(L_REC_VAL(I).AMOUNT,20,' ')
                         ||'     '||L_REC_VAL(I).INTERNAL_DESCRIPTION );

              END IF;
              END LOOP;
              END IF;*/
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            fnd_file.put_line (1, 'Error in the calling XXARG_HR_HSA_PKG  ');
      END;

      --
      fnd_file.put_line (debug_on, 'All source records processed.');

      --
      IF l_success_count = 0
      THEN
         --
         fnd_file.put_line
               (1,
                'No batch lines created.  Hence delete the batch header now.'
               );

         --
         BEGIN
            DELETE FROM pay_batch_headers
                  WHERE batch_id = l_batch_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line (1, 'ERROR! Removing the pay batch.');
               fnd_file.put_line (1, 'ERRMSG: ' || SQLERRM);
               fnd_file.put_line (1,
                                     'Please delete the batch '
                                  || l_batch_name
                                  || ' manually.'
                                 );
         END;
      --
      END IF;

      --
      fnd_file.put_line (debug_on,
                         'Printing Statistics, performing commit and exiting.'
                        );
      --
      COMMIT;
      --
      --
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, '**********  Program Stats  **********');
      fnd_file.put_line (1, ' ');

      IF l_success_count > 0
      THEN
         fnd_file.put_line (1, 'BEE Batch Name created: ' || l_batch_name);
         fnd_file.put_line (2, 'BEE HB Batch Name created: ' || l_batch_name);
      END IF;

      fnd_file.put_line (1,
                         'Success Records:   ' || TO_CHAR (l_success_count));
      fnd_file.put_line (1, 'Error Records:     ' || TO_CHAR (l_error_count));
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1,
                            'Total Source Records Processed: '
                         || TO_CHAR (l_count)
                        );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, '************  End Stats  ************');
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, ' ');

      --
      IF l_error_count > 0
      THEN
         generate_hb_error_report;
         retcode := 1;
      ELSE
         retcode := 0;
      END IF;

      generate_hb_end_report;
   --
   EXCEPTION
      WHEN g_end_program
      THEN
         retcode := 2;
         fnd_file.put_line (1, ' ');
         fnd_file.put_line (1,
                            'ERROR!! Processing the Mercer Inbound HB file.'
                           );
         fnd_file.put_line (1, 'ERRMSG: ' || errbuf);
         ROLLBACK;
      WHEN OTHERS
      THEN
         errbuf := SQLERRM;
         retcode := 2;
         ROLLBACK;
   END process_hb_file;

--
--  LAKSHA ADDED THIS CODE FOR ER HSA ELEMENT PROCESSING
--
   PROCEDURE PROCESS_HSA_FILE (
      ERRBUF                   OUT      VARCHAR2,
      RETCODE                  OUT      NUMBER,
      P_BUSINESS_GROUP_ID      IN       NUMBER,
      P_MOVE_FLAG              IN       VARCHAR2,
      P_RERUN_FLAG             IN       VARCHAR2,
      P_OPEN_ENROLLMENT_FLAG   IN       VARCHAR2,
      P_DEBUG_FLAG             IN       VARCHAR2
   )
   IS
--
      L_COUNT                   NUMBER          := 0;
      L_SUCCESS_COUNT           NUMBER          := 0;
      L_ERROR_COUNT             NUMBER          := 0;
--
      L_FULL_NAME               VARCHAR2 (1000);
      L_PERSON_ID               NUMBER;
      L_ASSIGNMENT_ID           NUMBER;
      L_ASSIGNMENT_NUMBER       VARCHAR2 (30);
      L_BATCH_ID                NUMBER          := 0;
      L_BATCH_NAME              VARCHAR2 (30)   := NULL;
      L_OBJECT_VERSION_NUMBER   NUMBER          := 0;
      L_BATCH_LINE_ID           NUMBER          := 0;
--
      L_SEQ                     NUMBER          := 0;
      L_D_SEQ                   NUMBER          := 0;
      L_S_SEQ                   NUMBER          := 0;
      L_DEFAULT_DED             VARCHAR2 (100);
      L_P_SEQ                   NUMBER          := 0;
      L_DEFAULT_PER             VARCHAR2 (100);
      L_DEFAULT_SEP             VARCHAR2 (100);
      L_DED_AMT1                VARCHAR2 (100)  := NULL;
      L_DED_AMT2                VARCHAR2 (100)  := NULL;
      L_DED_AMT3                VARCHAR2 (100)  := NULL;
      L_DED_AMT4                VARCHAR2 (100)  := NULL;
      L_DED_AMT5                VARCHAR2 (100)  := NULL;
      L_DED_AMT6                VARCHAR2 (100)  := NULL;
      L_DED_AMT7                VARCHAR2 (100)  := NULL;
      L_DED_AMT8                VARCHAR2 (100)  := NULL;
      L_DED_AMT9                VARCHAR2 (100)  := NULL;
      L_DED_AMT10               VARCHAR2 (100)  := NULL;
      L_DED_AMT11               VARCHAR2 (100)  := NULL;
      L_DED_AMT12               VARCHAR2 (100)  := NULL;
      L_DED_AMT13               VARCHAR2 (100)  := NULL;
      L_DED_AMT14               VARCHAR2 (100)  := NULL;
      L_DED_AMT15               VARCHAR2 (100)  := NULL;
      L_ERHSA_AMT               NUMBER          := NULL;
      L_DED_AMT                 NUMBER          := NULL;
      L_ANNUAL_DED_AMT          NUMBER          := NULL;
      L_MONTHS                  NUMBER          := NULL;
      L_PAYROLL_ID              NUMBER          := NULL;
      L_CHECK_DATE              DATE;
      L_EFFECTIVE_DATE          DATE;
      L_EFFECTIVE_YEAR          NUMBER;
      HSA_RETCODE               NUMBER;
      HSA_ERRBUF                VARCHAR2 (1000);
      L_HSA_BATCH_ID            NUMBER;
      L_BUSINESS_GROUP_ID       NUMBER;
      L_ELEMENT_TYPE_ID         NUMBER;
      L_ELEMENT_LINK_ID         NUMBER;
      V_COUNT                   NUMBER          := 0;
      L_ELE_FLAG                VARCHAR2 (1);
      L_ELEMENT_ENTRY_ID        NUMBER          := 0;
      L_OVN                     NUMBER          := 0;
      L_EFF_START_DATE          DATE            := NULL;
      L_EFF_END_DATE            DATE            := NULL;
      L_WARNING                 BOOLEAN;
      L_STATUS                  NUMBER;
      --V_LAK_COUNT               NUMBER          := 0;
     -- V_LAKSHA_OUTPUT           VARCHAR2 (1000);

--
--
                  CURSOR SRC_RECS IS
                     SELECT     *
                           FROM XXARG_MERCER_INBOUND_HSA_STAGE A
                          WHERE 1 = 1
                            AND NVL (A.PROCESS_FLAG, 'N') IN ('N', 'E')
                            AND (   (P_OPEN_ENROLLMENT_FLAG = 'Y'
                                     AND DEDUCTION_AMOUNT <> 0
                                    )
                                 OR (P_OPEN_ENROLLMENT_FLAG = 'N')
                                )
                     FOR UPDATE;

      CURSOR C1 (P_ELEMENT_NAME VARCHAR2)
      IS
         SELECT   INPUT_VALUE_ID, NAME, DISPLAY_SEQUENCE,
                  PETF.ELEMENT_TYPE_ID                   --,PIVF.DEFAULT_VALUE
                                      ,
                  HR_GENERAL.DECODE_LOOKUP (LOOKUP_TYPE,
                                            DEFAULT_VALUE
                                           ) DEFAULT_VALUE,
                  DEFAULT_VALUE PER
             FROM PAY_INPUT_VALUES_F PIVF, PAY_ELEMENT_TYPES_F PETF
            WHERE PIVF.ELEMENT_TYPE_ID = PETF.ELEMENT_TYPE_ID
              AND TRUNC (SYSDATE) BETWEEN TRUNC (PETF.EFFECTIVE_START_DATE)
                                      AND TRUNC (PETF.EFFECTIVE_END_DATE)
              AND TRUNC (SYSDATE) BETWEEN TRUNC (PIVF.EFFECTIVE_START_DATE)
                                      AND TRUNC (PIVF.EFFECTIVE_END_DATE)
              AND PETF.ELEMENT_NAME = TRIM (P_ELEMENT_NAME)
         ORDER BY DISPLAY_SEQUENCE, NAME;

         V_LAK_COUNT NUMBER:=0;


   BEGIN

      -- LAKSHA
       --
      FND_FILE.PUT_LINE (FND_FILE.LOG,
                         '====== LAKSHA CODE -- STARTED ========= '
                        );
      FND_FILE.PUT_LINE (FND_FILE.LOG,
                            'MERCER HSA BENEFITS INBOUND PROCESS STARTED: '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                        );
      FND_FILE.PUT_LINE (FND_FILE.LOG, ' ');
      FND_FILE.PUT_LINE
                 (FND_FILE.LOG,
                  'INSIDE PACKAGE XXARG_MERCER_INBOUND_PKG.PROCESS_HSA_FILE..'
                 );
      FND_FILE.PUT_LINE (FND_FILE.LOG,
                            'RERUN FLAG IS '
                         || P_RERUN_FLAG
                         || '  :      MOVE FLAG   =  '
                         || P_MOVE_FLAG
                         || '  :      BUS GRP ID =  '
                         || P_BUSINESS_GROUP_ID
                         || '  :      OPEN ENRT FLAG =  '
                         || P_OPEN_ENROLLMENT_FLAG
                         || '  :      DBUG FLAG =  '
                         || P_DEBUG_FLAG
                        );

      -- LAKSHA


      --
      G_DEBUG_FLAG := P_DEBUG_FLAG;
      --
      ERRBUF := NULL;
      RETCODE := 0;
      V_LAK_COUNT := 0;
      --
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE (1,
                            'MERCER HSA BENEFITS INBOUND PROCESS STARTED: '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                        );
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE
                 (DEBUG_ON,
                  'INSIDE PACKAGE XXARG_MERCER_INBOUND_PKG.PROCESS_HSA_FILE..'
                 );
      FND_FILE.PUT_LINE (DEBUG_ON, 'RERUN FLAG IS ' || P_RERUN_FLAG);
      --



      --
      IF P_RERUN_FLAG = 'N'
      THEN
         IF P_MOVE_FLAG = 'Y'
         THEN
            BEGIN
               L_STATUS := 0;
               L_STATUS:= FILE_TRANSFER('ERHSA_HB.csv','ERHSA_HB.csv');

               IF L_STATUS = 1
               THEN
                  ERRBUF := 'ERROR IN THE FTP PROGROM ' || SQLERRM;
                  RETCODE := 2;
                  RAISE G_END_PROGRAM;
               END IF;


            EXCEPTION
               WHEN OTHERS
               THEN
                  ERRBUF := 'ERROR IN THE FTP PROGROM ' || SQLERRM;
                  RETCODE := 2;
                  RAISE G_END_PROGRAM;
            END;
         END IF;

         --
         FND_FILE.PUT_LINE (DEBUG_ON,
                            'CLEANUP THE STAGING TABLE AFTER PREVIOUS RUN'
                           );
         FND_FILE.PUT_LINE
            (DEBUG_ON,
             'CHANGING ALL UN-PROCESSED RECS FROM N TO CN AND ERROR RECORDS FROM E TO CE'
            );
         --

         --LAKSHA
         FND_FILE.PUT_LINE (FND_FILE.LOG,
                            'CLEANUP THE STAGING TABLE AFTER PREVIOUS RUN'
                           );
         FND_FILE.PUT_LINE
            (FND_FILE.LOG,
             'CHANGING ALL UN-PROCESSED RECS FROM N TO CN AND ERROR RECORDS FROM E TO CE'
            );

         -- LAKSHA
         --
         BEGIN
            FND_FILE.PUT_LINE (FND_FILE.LOG,
                               'BEFORE UPDATING THE STAGE TAB   '
                              );

            UPDATE XXARG_MERCER_INBOUND_HSA_STAGE
               SET PROCESS_FLAG = 'C' || NVL (PROCESS_FLAG, 'N')
             WHERE NVL (PROCESS_FLAG, 'N') IN ('N', 'E');

            --
            COMMIT;
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               FND_FILE.PUT_LINE (1, ' ');
               FND_FILE.PUT_LINE
                     (1,
                      'ERROR UPDATING THE DATA STAGE TABLE DURING CLEAN UP..'
                     );
               FND_FILE.PUT_LINE
                  (FND_FILE.LOG,
                   '-- LAK  --ERROR UPDATING THE DATA STAGE TABLE DURING CLEAN UP..'
                  );
               ERRBUF := SQLERRM;
               RAISE G_END_PROGRAM;
         END;

         --
         FND_FILE.PUT_LINE
                       (FND_FILE.LOG,
                        'LAK -- NOW LOAD THE STAGING TABLE FROM THE DATA FILE'
                       );

         --
         BEGIN
            -- LAKSHA
            V_LAK_COUNT := V_LAK_COUNT + 1;
            FND_FILE.PUT_LINE (FND_FILE.LOG,
                               'BEFORE INSERTING INTO STAGING TABLE'
                              );
                        FND_FILE.PUT_LINE (FND_FILE.LOG,
                                           'REC COUNT    =  ' || V_LAK_COUNT);

            -- LAKSHA
            INSERT INTO XXARG_MERCER_INBOUND_HSA_STAGE
                        (TAX_ID_NUMBER                                    -- 1
            ,
                         ONE_CODE                                         -- 2
                                 ,
                         AS_OF_DATE                                       -- 3
                                   ,
                         EFFECTIVE_DATE                                   -- 4
                                       ,
                         EMPLOYEE_NUMBER                                  -- 5
                                        ,
                         DEDUCTION_CLASS                                  -- 6
                                        ,
                         DEDUCTION_CODE                                   -- 7
                                       ,
                         DEDUCTION_AMOUNT                                 -- 8
                                         ,
                         ANNUAL_GOAL_AMOUNT                               -- 9
                                           ,
                         CREATION_DATE                                   -- 10
                                      ,
                         CREATED_BY                                      -- 11
                                   ,
                         LAST_UPDATE_DATE                                -- 12
                                         ,
                         LAST_UPDATED_BY                                 -- 13
                                        ,
                         LAST_UPDATE_LOGIN                               -- 14
                                          ,
                         PROGRAM_ID                                      -- 15
                                   ,
                         REQUEST_ID                                      -- 16
                                   ,
                         PROCESS_FLAG                                    -- 17
                                     ,
                         PROCESS_MESSAGE                                 -- 18
                                        ,
                         PBH_BATCH_ID                                    -- 19
                                     ,
                         PBL_BATCH_LINE_ID                               -- 20
                                          )
               SELECT TAX_ID_NUMBER                                       -- 1
                                   ,
                      ONE_CODE                                            -- 2
                              ,
                      TO_DATE (AS_OF_DATE, 'MMDDYYYY')                    -- 3
                                                      ,
                      TO_DATE (EFFECTIVE_DATE, 'MMDDYYYY')                 --4
                                                          ,
                      EMPLOYEE_NUMBER                                     -- 5
                                     ,
                      DEDUCTION_CLASS                                      --6
                                     ,
                      TRIM (DEDUCTION_CODE)                               -- 7
                                           ,
                      DEDUCTION_AMOUNT                                     --8
                                      ,
                      REPLACE (REPLACE (ANNUAL_GOAL_AMOUNT, CHR (13), ''),
                               CHR (10),
                               ''
                              )                                            --9
                               ,
                      SYSDATE                                            -- 10
                             ,
                      FND_GLOBAL.USER_ID                                 -- 11
                                        ,
                      SYSDATE                                            -- 12
                             ,
                      FND_GLOBAL.USER_ID                                 -- 13
                                        ,
                      FND_GLOBAL.LOGIN_ID                                 --14
                                         ,
                      FND_GLOBAL.CONC_PROGRAM_ID                         -- 15
                                                ,
                      FND_GLOBAL.CONC_REQUEST_ID                         -- 16
                                                ,
                      'N'                                                -- 17
                         ,
                      NULL       --18
                             ,
                      NULL  -- 19
                       ,
                      NULL       -- 20
                 FROM XXARG_MERCER_INBOUND_HSA_FILE
            --                 WHERE EMPLOYEE_NUMBER = '0853868'
                    --                 ROWNUM < 1000
                    ;

            --
            COMMIT;
                            FND_FILE.PUT_LINE (FND_FILE.LOG,
                                               'LAK-COUNT OF RECORDS       ' || V_LAK_COUNT
                                              );
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               FND_FILE.PUT_LINE (1, ' ');
               FND_FILE.PUT_LINE
                   (1,
                    'LAKSHAD -- ERROR INSERTING DATA FROM SOURCE FILE INTO STAGING TABLE'||SUBSTR(SQLERRM,1,220)
                   );
               ERRBUF := SQLERRM;
               RAISE G_END_PROGRAM;
         END;
      END IF;

      --
      --
      FND_FILE.PUT_LINE (DEBUG_ON, 'OPENING THE DETAIL DATA CURSOR');
      FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK - BEFORE OPENING THE CURSOR   ');

      --
      FOR CR IN SRC_RECS
      LOOP
         FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK - IN THE LOOP  ');
         --
         ERRBUF := NULL;
         RETCODE := 0;
         L_COUNT := L_COUNT + 1;
         L_DED_AMT1 := NULL;
         L_DED_AMT2 := NULL;
         L_DED_AMT3 := NULL;
         L_DED_AMT4 := NULL;
         L_DED_AMT5 := NULL;
         L_DED_AMT6 := NULL;
         L_DED_AMT7 := NULL;
         L_DED_AMT8 := NULL;
         L_DED_AMT9 := NULL;
         L_DED_AMT10 := NULL;
         L_DED_AMT11 := NULL;
         L_DED_AMT12 := NULL;
         L_DED_AMT13 := NULL;
         L_DED_AMT14 := NULL;
         L_DED_AMT15 := NULL;
         L_ERHSA_AMT := NULL;
         L_DED_AMT := NULL;
         L_ANNUAL_DED_AMT := NULL;
         L_MONTHS := NULL;
         L_PAYROLL_ID := NULL;
         L_CHECK_DATE := NULL;
         L_EFFECTIVE_DATE := NULL;
         L_EFFECTIVE_YEAR := NULL;
         L_BUSINESS_GROUP_ID := NULL;
         L_ELEMENT_TYPE_ID := NULL;
         L_ELEMENT_LINK_ID := NULL;
         L_ELEMENT_ENTRY_ID := NULL;
         L_OVN := NULL;
         L_BATCH_LINE_ID := 0;

         --
         IF L_COUNT = 1
         THEN
            --
            FND_FILE.PUT_LINE (DEBUG_ON, 'CREATING PAY BATCH HEADER..');
            --
            FND_FILE.PUT_LINE (FND_FILE.LOG,
                               'LAK -CREATING THE BATCH HEADER '
                              );

            --
            BEGIN
               SELECT XXARG_HR_HSA_BATCH_ID_S.NEXTVAL
                 INTO L_HSA_BATCH_ID
                 FROM SYS.DUAL;

               FND_FILE.PUT_LINE (FND_FILE.LOG,
                                  'LAK - BATCH ID ' || L_HSA_BATCH_ID
                                 );
            EXCEPTION
               WHEN OTHERS THEN
                  FND_FILE.PUT_LINE (1, 'ERROR WHILE GETTING SEQUENCE HSA');
            END;

            BEGIN
               --
               L_BATCH_NAME :=
                         'MERCER_HSA_' || TO_CHAR (SYSDATE, 'YYYYMMDD_SSSSS');
               FND_FILE.PUT_LINE (FND_FILE.LOG,
                                  'LAK - BATCH NAME   ' || L_BATCH_NAME
                                 );
               --
               PAY_BATCH_ELEMENT_ENTRY_API.CREATE_BATCH_HEADER
                    (P_SESSION_DATE                => TRUNC (CR.EFFECTIVE_DATE),
                     P_BATCH_NAME                  => L_BATCH_NAME,
                     P_BUSINESS_GROUP_ID           => P_BUSINESS_GROUP_ID,
                     P_ACTION_IF_EXISTS            => 'U',
                     P_BATCH_SOURCE                => 'MERCER HSA FILE',
                     P_BATCH_REFERENCE             => FND_DATE.DATE_TO_CANONICAL
                                                                      (SYSDATE),
                     P_DATE_EFFECTIVE_CHANGES      => 'U',
                     P_BATCH_ID                    => L_BATCH_ID,
                     P_OBJECT_VERSION_NUMBER       => L_OBJECT_VERSION_NUMBER
                    );
               --
               FND_FILE.PUT_LINE (DEBUG_ON,
                                     'BATCH ID CREATED SUCCESSFULLY: '
                                  || TO_CHAR (L_BATCH_ID)
                                 );
               FND_FILE.PUT_LINE (DEBUG_ON, 'BATCH NAME: ' || L_BATCH_NAME);
               FND_FILE.PUT_LINE (DEBUG_ON, ' ');
               FND_FILE.PUT_LINE (FND_FILE.LOG,
                                  'LAK - BATCH CREATED SUCCCESSSSSFULLLLLLY'
                                 );
               FND_FILE.PUT_LINE (FND_FILE.LOG,
                                     'LAK -BATCH ID CREATED SUCCESSFULLY: '
                                  || TO_CHAR (L_BATCH_ID)
                                 );
            --
            EXCEPTION
               WHEN OTHERS THEN
                  FND_FILE.PUT_LINE (1, ' ');
                  FND_FILE.PUT_LINE (1, 'ERROR CREATING PAY BATCH HEADER');
                  ERRBUF := 'ERROR CREATING PAY BATCH HEADER ' || SQLERRM;
                  RAISE G_END_PROGRAM;
            END;
         --
         END IF;

         --
         FND_FILE.PUT_LINE (DEBUG_ON, ' ');
         FND_FILE.PUT_LINE (DEBUG_ON, 'TAX ID#: ' || CR.TAX_ID_NUMBER);
         FND_FILE.PUT_LINE (DEBUG_ON, 'EMPLOYEE#: ' || CR.EMPLOYEE_NUMBER);
         FND_FILE.PUT_LINE (DEBUG_ON,
                               'EFFECTIVE DATE: '
                            || TO_CHAR (CR.EFFECTIVE_DATE, 'DD-MON-RRRR')
                           );
         FND_FILE.PUT_LINE (DEBUG_ON, 'DEDUCTION CODE: ' || CR.DEDUCTION_CODE);
         FND_FILE.PUT_LINE (DEBUG_ON,
                               'DEDUCTION AMOUNT: '
                            || TO_CHAR (CR.DEDUCTION_AMOUNT)
                           );
         FND_FILE.PUT_LINE (DEBUG_ON, 'FETCHING THE EMPLOYEE DETAILS..');

         --  LAKSHADI ADDED ON DEC 21 2010
         CR.EFFECTIVE_DATE := TO_CHAR (CR.EFFECTIVE_DATE, 'DD-MON-RRRR') ;

         -- LAKSHA --- ADDED FOR TESTING
            --         CR.EFFECTIVE_DATE := TO_DATE('01-JAN-2011','DD-MON-YYYY') ;

            --         BEGIN
            --            CR.EFFECTIVE_DATE := TO_CHAR(TO_DATE(CR.EFFECTIVE_DATE,'MMDDYYYY'),'DD-MON-YYYY') ;  --TO_DATE(CR.EFFECTIVE_DATE,'DD-MON-YYYY') ;
            --         EXCEPTION
            --            WHEN OTHERS THEN
            --                FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK ==== ERROR IN EFFECTIVE DATE FORMATTING   '||SUBSTR(SQLERRM,1,220) );
            --         END;

         --
         FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK ==============================================================');
         FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK - EMPLOYEE NUMBER FROM CURSOR    '||    CR.EMPLOYEE_NUMBER
                            ||'   :    '||'TAX UNIT ID #     '||       CR.TAX_ID_NUMBER
                            ||'   :    '||'EFFECTIVE DATE - AFTER FORMATTING     '||CR.EFFECTIVE_DATE
                            ) ;
         FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK ==============================================================');

         BEGIN
            --
            SELECT DISTINCT
                  PAPF.FULL_NAME
                , PAPF.PERSON_ID
                , PAAF.ASSIGNMENT_ID
                , PAAF.ASSIGNMENT_NUMBER
                , PAAF.PAYROLL_ID
                , PAAF.BUSINESS_GROUP_ID
            INTO L_FULL_NAME
                , L_PERSON_ID
                , L_ASSIGNMENT_ID
                , L_ASSIGNMENT_NUMBER
                , L_PAYROLL_ID
                , L_BUSINESS_GROUP_ID
            FROM
                    PER_ALL_PEOPLE_F           PAPF
                ,   PER_ALL_ASSIGNMENTS_F      PAAF
            WHERE   --PAPF.EMPLOYEE_NUMBER = '1038531'   --CR.EMPLOYEE_NUMBER
                    LTRIM (PAPF.EMPLOYEE_NUMBER, 0) = LTRIM (TRIM (CR.EMPLOYEE_NUMBER), 0)
              AND   XXARG_UNFRMT_NATL_ID_FNC (PAPF.NATIONAL_IDENTIFIER) = XXARG_UNFRMT_NATL_ID_FNC (CR.TAX_ID_NUMBER)
              AND   PAAF.PERSON_ID = PAPF.PERSON_ID
              --AND   TO_DATE('01-JAN-2011','DD-MON-YYYY') BETWEEN PAPF.EFFECTIVE_START_DATE AND PAPF.EFFECTIVE_END_DATE
            --  AND   TO_DATE('01-JAN-2011','DD-MON-YYYY') BETWEEN PAAF.EFFECTIVE_START_DATE AND PAAF.EFFECTIVE_END_DATE
            AND   CR.EFFECTIVE_DATE BETWEEN TRUNC (PAPF.EFFECTIVE_START_DATE) AND   TRUNC (PAPF.EFFECTIVE_END_DATE)
            AND   CR.EFFECTIVE_DATE BETWEEN TRUNC (PAAF.EFFECTIVE_START_DATE) AND TRUNC (PAAF.EFFECTIVE_END_DATE)
              AND   PAAF.PRIMARY_FLAG = 'Y';
            --
            FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK - EMPLOYEE NAME:    ' || L_FULL_NAME);
            FND_FILE.PUT_LINE (DEBUG_ON, 'EMPLOYEE NAME: ' || L_FULL_NAME);
            --
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK**** - IN NO DATA FOUND  '||SUBSTR(SQLERRM,1,220)||'   :   ' ||LTRIM (TRIM (CR.EMPLOYEE_NUMBER), 0 )||'   :  SSN =  '||XXARG_UNFRMT_NATL_ID_FNC (CR.TAX_ID_NUMBER));
            WHEN OTHERS THEN
               FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK - SSN   '|| 'ORACLE ERROR CODE  '||SUBSTR(SQLERRM,1,220)||'   :   ' ||XXARG_UNFRMT_NATL_ID_FNC (CR.TAX_ID_NUMBER)
                                        ||' EMP#    = '||LTRIM (TRIM (CR.EMPLOYEE_NUMBER), 0));

               FND_FILE.PUT_LINE (1, ' ');
               FND_FILE.PUT_LINE
                          (1,
                              'ERROR FETCHING THE EMPLOYEE DETAILS FOR SSN  '
                           || XXARG_UNFRMT_NATL_ID_FNC (CR.TAX_ID_NUMBER)
                          );
               ERRBUF := 'ERROR FETCHING THE EMPLOYEE DETAILS  ' || SQLERRM;
               RETCODE := 1;
               GOTO END_OF_LOOP;
         END;

         IF CR.DEDUCTION_AMOUNT = 0 THEN
            BEGIN
               SELECT PEEF.ELEMENT_ENTRY_ID, PEEF.OBJECT_VERSION_NUMBER
                 INTO L_ELEMENT_ENTRY_ID, L_OVN
                 FROM PER_ALL_PEOPLE_F PAPF,
                      PER_ALL_ASSIGNMENTS_F PAAF,
                      PAY_ELEMENT_ENTRIES_F PEEF,
                      PAY_ELEMENT_LINKS_F PELF,
                      PAY_ELEMENT_TYPES_F PETF
                WHERE PAPF.PERSON_ID = PAAF.PERSON_ID
                  AND PAAF.ASSIGNMENT_ID = PEEF.ASSIGNMENT_ID
                  AND PEEF.ELEMENT_LINK_ID = PELF.ELEMENT_LINK_ID
                  AND PELF.ELEMENT_TYPE_ID = PETF.ELEMENT_TYPE_ID
                  AND PEEF.ENTRY_TYPE = 'E'
                  AND UPPER (PETF.ELEMENT_NAME) LIKE
                                              UPPER (TRIM (CR.DEDUCTION_CODE))
                  AND PAAF.ASSIGNMENT_ID = L_ASSIGNMENT_ID
                  AND PEEF.EFFECTIVE_END_DATE <> CR.EFFECTIVE_DATE
                  AND CR.EFFECTIVE_DATE BETWEEN PAPF.EFFECTIVE_START_DATE
                                            AND PAPF.EFFECTIVE_END_DATE
                  AND CR.EFFECTIVE_DATE BETWEEN PAAF.EFFECTIVE_START_DATE
                                            AND PAAF.EFFECTIVE_END_DATE
                  AND CR.EFFECTIVE_DATE BETWEEN PEEF.EFFECTIVE_START_DATE
                                            AND PEEF.EFFECTIVE_END_DATE
                  AND CR.EFFECTIVE_DATE BETWEEN PELF.EFFECTIVE_START_DATE
                                            AND PELF.EFFECTIVE_END_DATE
                  AND CR.EFFECTIVE_DATE BETWEEN PETF.EFFECTIVE_START_DATE
                                            AND PETF.EFFECTIVE_END_DATE;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  FND_FILE.PUT_LINE (FND_FILE.LOG, 'LAK - IN NO DATA FOUND - DEDUCTION AMOUNT  '||SUBSTR(SQLERRM,1,220)||'   : ASGID =  ' ||L_ASSIGNMENT_ID );
                  RETCODE := 0;
                  GOTO END_OF_LOOP;
                WHEN OTHERS THEN
                  FND_FILE.PUT_LINE
                             (1,
                              'ERROR! FETCHING THE ELEMENT ENTRY DETAILS... '
                             );
                  FND_FILE.PUT_LINE (1, 'EMP#: ' || TRIM (CR.EMPLOYEE_NUMBER));
                  ERRBUF :=
                      'ERROR! FETCHING THE ELEMENT ENTRY DETAILS. ' || SQLERRM;
                  RETCODE := 1;
                  GOTO END_OF_LOOP;
            END;

            --
            --
            FND_FILE.PUT_LINE
                         (DEBUG_ON,
                          'NOW CALLING THE API TO DELETE THE ELEMENT ENTRY.. '
                         );

            --
            BEGIN
               PAY_ELEMENT_ENTRY_API.DELETE_ELEMENT_ENTRY
                                 (P_DATETRACK_DELETE_MODE      => 'DELETE',
                                  P_EFFECTIVE_DATE             => CR.EFFECTIVE_DATE,
                                  P_ELEMENT_ENTRY_ID           => L_ELEMENT_ENTRY_ID,
                                  P_OBJECT_VERSION_NUMBER      => L_OVN,
                                  P_EFFECTIVE_START_DATE       => L_EFF_START_DATE,
                                  P_EFFECTIVE_END_DATE         => L_EFF_END_DATE,
                                  P_DELETE_WARNING             => L_WARNING
                                 );
               L_BATCH_LINE_ID := 1;
            EXCEPTION
               WHEN OTHERS
               THEN
                  FND_FILE.PUT_LINE
                             (DEBUG_ON,
                              'ERROR! CALLING THE DELETE_ELEMENT_ENTRY API..'
                             );
                  FND_FILE.PUT_LINE (DEBUG_ON, 'ERRMSG: ' || SQLERRM);
                  ERRBUF :=
                     'ERROR! CALLING THE DELETE_ELEMENT_ENTRY API..'
                     || SQLERRM;
                  RETCODE := 1;
                  GOTO END_OF_LOOP;
            END;
         ELSE
            --
            --
            FND_FILE.PUT_LINE (DEBUG_ON,
                               'FETCH THE ELEMENT ENTRY VALUE DETAILS..'
                              );

            IF (   TRIM (CR.DEDUCTION_CODE) = 'ERHSA'
                OR TRIM (CR.DEDUCTION_CODE) = 'ERHSA1'
               )
            THEN
               BEGIN
                  V_COUNT := 0;
                  L_SEQ := 0;

                  FOR C1_REC IN C1 ((CR.DEDUCTION_CODE))
                  LOOP
                     V_COUNT := V_COUNT + 1;

                     IF (UPPER (C1_REC.NAME) = 'MAXIMUM AMOUNT')
                     THEN
                        L_SEQ := V_COUNT;
                     END IF;
                  END LOOP;

                  IF L_SEQ = 0
                  THEN
                     FND_FILE.PUT_LINE (1, ' ');
                     FND_FILE.PUT_LINE
                        (1,
                            'ERROR FETCHING THE ELEMENT INPUT VALUE MAXIMUM AMOUNT FOR ELEMENT '
                         || TRIM (CR.DEDUCTION_CODE)
                        );
                     ERRBUF :=
                           'ERROR FETCHING THE ELEMENT INPUT VALUE MAXIMUM AMOUNT FOR ELEMENT '
                        || SQLERRM;
                     RETCODE := 1;
                     GOTO END_OF_LOOP;
                  END IF;

                  --
                  FND_FILE.PUT_LINE
                     (DEBUG_ON,
                         'INPUT VALUE " MAX AMOUNT" HAS A DISPLAY SEQUENCE OF: '
                      || TO_CHAR (L_SEQ)
                     );
                  FND_FILE.PUT_LINE
                             (DEBUG_ON,
                                 'HENCE PAY_VALUE_'
                              || TO_CHAR (L_SEQ)
                              || ' WILL BE USED TO POPULATE THE DEDUCTION AMOUNT'
                             );
                  --
                  L_ANNUAL_DED_AMT := CR.ANNUAL_GOAL_AMOUNT;

                  IF L_SEQ = 1
                  THEN
                     L_DED_AMT1 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 2
                  THEN
                     L_DED_AMT2 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 3
                  THEN
                     L_DED_AMT3 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 4
                  THEN
                     L_DED_AMT4 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 5
                  THEN
                     L_DED_AMT5 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 6
                  THEN
                     L_DED_AMT6 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 7
                  THEN
                     L_DED_AMT7 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 8
                  THEN
                     L_DED_AMT8 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 9
                  THEN
                     L_DED_AMT9 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 10
                  THEN
                     L_DED_AMT10 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 11
                  THEN
                     L_DED_AMT11 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 12
                  THEN
                     L_DED_AMT12 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 13
                  THEN
                     L_DED_AMT13 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 14
                  THEN
                     L_DED_AMT14 := L_ANNUAL_DED_AMT;
                  ELSIF L_SEQ = 15
                  THEN
                     L_DED_AMT15 := L_ANNUAL_DED_AMT;
                  END IF;
               --
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     FND_FILE.PUT_LINE (1, ' ');
                     FND_FILE.PUT_LINE
                        (1,
                            'ERROR FETCHING THE ELEMENT INPUT VALUE MAXIMUM AMOUNT FOR ELEMENT '
                         || TRIM (CR.DEDUCTION_CODE)
                        );
                     ERRBUF :=
                           'ERROR FETCHING THE ELEMENT INPUT VALUE MAXIMUM AMOUNT FOR ELEMENT '
                        || SQLERRM;
                     RETCODE := 1;
                     GOTO END_OF_LOOP;
               END;
            END IF;

            FND_FILE.PUT_LINE (DEBUG_ON,
                               'FETCH THE ELEMENT ENTRY VALUE DETAILS.'
                              );

            BEGIN
               V_COUNT := 0;
               L_SEQ := 0;
               L_D_SEQ := 0;
               L_S_SEQ := 0;
               L_DEFAULT_DED := NULL;
               L_DEFAULT_SEP := NULL;
               L_P_SEQ := 0;
               L_DEFAULT_PER := NULL;

             FND_FILE.PUT_LINE(DEBUG_ON,'FETCH THE ELEMENT ENTRY VALUE DETAILS1.');
               FOR C1_REC IN C1 ((CR.DEDUCTION_CODE))
               LOOP
                  FND_FILE.PUT_LINE(DEBUG_ON,'FETCH THE ELEMENT ENTRY VALUE DETAILS2.');
                  V_COUNT := V_COUNT + 1;
                  L_ELEMENT_TYPE_ID := C1_REC.ELEMENT_TYPE_ID;

                  IF (UPPER (C1_REC.NAME) = 'AMOUNT')
                  THEN
                     L_SEQ := V_COUNT;
                  END IF;

                  IF (UPPER (C1_REC.NAME) = 'DEDUCTION PROCESSING')
                  THEN
                     L_D_SEQ := V_COUNT;
                     L_DEFAULT_DED := C1_REC.DEFAULT_VALUE;
                  END IF;

                  IF (UPPER (C1_REC.NAME) = 'SEPARATE CHECK')
                  THEN
                     L_S_SEQ := V_COUNT;
                     L_DEFAULT_SEP := C1_REC.DEFAULT_VALUE;
                  END IF;

                  IF (   TRIM (CR.DEDUCTION_CODE) = 'HIMEDA'
                      OR TRIM (CR.DEDUCTION_CODE) = 'HIMEDB'
                     )
                  THEN
                     IF (UPPER (C1_REC.NAME) = 'PERCENTAGE')
                     THEN
                        L_P_SEQ := V_COUNT;
                        L_DEFAULT_PER := C1_REC.PER;
                     END IF;
                  END IF;
               END LOOP;

               IF (   TRIM (CR.DEDUCTION_CODE) = 'HIMEDA'
                   OR TRIM (CR.DEDUCTION_CODE) = 'HIMEDB'
                  )
               THEN
                  NULL;
               ELSE
                  IF L_SEQ = 0
                  THEN
                     FND_FILE.PUT_LINE (1, ' ');
                     FND_FILE.PUT_LINE
                        (1,
                            'ERROR FETCHING THE ELEMENT INPUT VALUE AMOUNT FOR ELEMENT '
                         || TRIM (CR.DEDUCTION_CODE)
                        );
                     ERRBUF :=
                           'ERROR FETCHING THE ELEMENT INPUT VALUE  AMOUNT FOR ELEMENT '
                        || SQLERRM;
                     RETCODE := 1;
                     GOTO END_OF_LOOP;
                  END IF;
               END IF;


               --
               FND_FILE.PUT_LINE
                        (DEBUG_ON,
                            'INPUT VALUE "AMOUNT" HAS A DISPLAY SEQUENCE OF: '
                         || TO_CHAR (L_SEQ)
                        );
               FND_FILE.PUT_LINE
                             (DEBUG_ON,
                                 'HENCE PAY_VALUE_'
                              || TO_CHAR (L_SEQ)
                              || ' WILL BE USED TO POPULATE THE DEDUCTION AMOUNT'
                             );

               ---CHECKING FOR TESTING PURPOSE
               BEGIN
                  L_ELEMENT_LINK_ID :=
                     HR_ENTRY_API.GET_LINK (L_ASSIGNMENT_ID,
                                            L_ELEMENT_TYPE_ID,
                                            CR.EFFECTIVE_DATE
                                           );

                  IF L_ELEMENT_LINK_ID IS NULL
                  THEN
                     FND_FILE.PUT_LINE (1, ' ');
                     FND_FILE.PUT_LINE
                                  (1,
                                      'ELEMENT LINK NOT FOUND FOR ELEMENT '
                                   || TRIM (CR.DEDUCTION_CODE)
                                   || ' SSN '
                                   || XXARG_UNFRMT_NATL_ID_FNC
                                                             (CR.TAX_ID_NUMBER)
                                  );
                     ERRBUF :=
                              'ELEMENT LINK NOT FOUND FOR ELEMENT ' || SQLERRM;
                     RETCODE := 1;
                     GOTO END_OF_LOOP;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     FND_FILE.PUT_LINE (1, ' ');
                     FND_FILE.PUT_LINE
                                  (1,
                                      'ELEMENT LINK NOT FOUND FOR ELEMENT '
                                   || TRIM (CR.DEDUCTION_CODE)
                                   || ' SSN '
                                   || XXARG_UNFRMT_NATL_ID_FNC
                                                             (CR.TAX_ID_NUMBER)
                                  );
                     ERRBUF :=
                              'ELEMENT LINK NOT FOUND FOR ELEMENT ' || SQLERRM;
                     RETCODE := 1;
                     GOTO END_OF_LOOP;
               END;

               --
               IF (   TRIM (CR.DEDUCTION_CODE) = 'ERHSA'
                   OR TRIM (CR.DEDUCTION_CODE) = 'ERHSA1'
                  )
               THEN
                  BEGIN
                     SELECT MIN (END_DATE)
                       INTO L_CHECK_DATE
                       FROM PER_TIME_PERIODS P
                      WHERE PAYROLL_ID = L_PAYROLL_ID
                        AND TO_CHAR (END_DATE, 'MON-YY') =
                                         TO_CHAR (CR.EFFECTIVE_DATE, 'MON-YY');
                  END;

                  L_EFFECTIVE_DATE := CR.EFFECTIVE_DATE;

                  IF (L_EFFECTIVE_DATE <
                                 '30-NOV-' || TO_CHAR (L_EFFECTIVE_DATE, 'YY')
                     )
                  THEN
                     L_EFFECTIVE_YEAR := TO_CHAR (L_EFFECTIVE_DATE, 'YY');
                  ELSE
                     L_EFFECTIVE_YEAR := TO_CHAR (L_EFFECTIVE_DATE, 'YY') + 1;
                  END IF;

                  IF L_CHECK_DATE >= L_EFFECTIVE_DATE
                  THEN
                     L_EFFECTIVE_DATE := TRUNC (CR.EFFECTIVE_DATE, 'MM');
                  ELSE
                     L_EFFECTIVE_DATE :=
                              ADD_MONTHS (TRUNC (CR.EFFECTIVE_DATE, 'MM'), 1);
                  END IF;

                  L_DED_AMT := CR.ANNUAL_GOAL_AMOUNT;
                  L_MONTHS :=
                     CEIL (MONTHS_BETWEEN ('30-NOV-' || L_EFFECTIVE_YEAR,
                                           L_EFFECTIVE_DATE
                                          )
                          );

                --                 -- LAKSHA ADDED ON JAN 03 2010
                --                 IF P_OPEN_ENROLLMENT_FLAG = 'Y'
                --                    L_DED_AMT := CR.DEDUCTION_AMOUNT;
                --                 ELSE
                --                    L_DED_AMT := CR.DEDUCTION_AMOUNT;
                --                 END IF ;
                --                 -- LAKSHA ADDED ON JAN 03 2010



                  IF P_OPEN_ENROLLMENT_FLAG = 'Y' THEN
                     L_ERHSA_AMT := CR.DEDUCTION_AMOUNT ; -- LAKSHA ADDED ON JAN 03 2010

                     IF L_ERHSA_AMT <> 0 THEN
                         BEGIN
                           INSERT INTO XXARG.XXARG_HR_HSA_DETAILS
                                       (HSA_BATCH_ID,
                                        PAYROLL_EMPLOYEE_ID,
                                        FUNDING_SOURCE,
                                        TRANSACTION_TYPE,
                                        AMOUNT,
                                        DESCRIPTION,
                                        EFFECTIVE_DATE,
                                        PAYROLL_ACTION_ID,
                                        ELEMENT_NAME,
                                        BUSINESS_GROUP_ID,
                                        INTERNAL_DESCRIPTION,
                                        CREATED_BY,
                                        CREATION_DATE,
                                        LAST_UPDATED_BY,
                                        LAST_UPDATE_DATE
                                       )
                                VALUES (L_HSA_BATCH_ID,
                                        XXARG_UNFRMT_NATL_ID_FNC
                                        (CR.TAX_ID_NUMBER),
                                        2, 'CR',
                                        L_ERHSA_AMT,
                                        'OPEN ENROLLMENT JANUARY FUNDING',
                                        '01-JAN-' || L_EFFECTIVE_YEAR, 0,
                                        TRIM (CR.DEDUCTION_CODE),
                                        L_BUSINESS_GROUP_ID,
                                        'MERCER INBOUND INTERFACE ER HSA Element Processing',
                                        FND_GLOBAL.USER_ID, SYSDATE,
                                        FND_GLOBAL.USER_ID,
                                        SYSDATE
                                       );
                        EXCEPTION
                           WHEN OTHERS THEN
                              FND_FILE.PUT_LINE
                                 (1,
                                     'ERROR WHILE LOADING ERHSA DETAILS INTO THE XXARG.XXARG_HR_HSA_DETAILS FOR EMPLOYEE '
                                  || L_ASSIGNMENT_NUMBER
                                 );
                        END;
                     END IF;
                  ELSE
                     L_ERHSA_AMT := CR.DEDUCTION_AMOUNT;  -- LAKSHA ADDED ON JAN 03 2010
                  END IF;

                  L_DED_AMT := CR.DEDUCTION_AMOUNT; -- LAKSHA ADDED ON JAN 03 2010
               ELSE
                  L_DED_AMT := CR.DEDUCTION_AMOUNT; -- LAKSHA ADDED ON JAN 03 2010
               END IF;

               IF L_SEQ = 1
               THEN
                  L_DED_AMT1 := L_DED_AMT;
               ELSIF L_SEQ = 2
               THEN
                  L_DED_AMT2 := L_DED_AMT;
               ELSIF L_SEQ = 3
               THEN
                  L_DED_AMT3 := L_DED_AMT;
               ELSIF L_SEQ = 4
               THEN
                  L_DED_AMT4 := L_DED_AMT;
               ELSIF L_SEQ = 5
               THEN
                  L_DED_AMT5 := L_DED_AMT;
               ELSIF L_SEQ = 6
               THEN
                  L_DED_AMT6 := L_DED_AMT;
               ELSIF L_SEQ = 7
               THEN
                  L_DED_AMT7 := L_DED_AMT;
               ELSIF L_SEQ = 8
               THEN
                  L_DED_AMT8 := L_DED_AMT;
               ELSIF L_SEQ = 9
               THEN
                  L_DED_AMT9 := L_DED_AMT;
               ELSIF L_SEQ = 10
               THEN
                  L_DED_AMT10 := L_DED_AMT;
               ELSIF L_SEQ = 11
               THEN
                  L_DED_AMT11 := L_DED_AMT;
               ELSIF L_SEQ = 12
               THEN
                  L_DED_AMT12 := L_DED_AMT;
               ELSIF L_SEQ = 13
               THEN
                  L_DED_AMT13 := L_DED_AMT;
               ELSIF L_SEQ = 14
               THEN
                  L_DED_AMT14 := L_DED_AMT;
               ELSIF L_SEQ = 15
               THEN
                  L_DED_AMT15 := L_DED_AMT;
               END IF;

               IF L_D_SEQ <> 0
               THEN
                  IF L_D_SEQ = 1
                  THEN
                     L_DED_AMT1 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 2
                  THEN
                     L_DED_AMT2 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 3
                  THEN
                     L_DED_AMT3 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 4
                  THEN
                     L_DED_AMT4 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 5
                  THEN
                     L_DED_AMT5 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 6
                  THEN
                     L_DED_AMT6 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 7
                  THEN
                     L_DED_AMT7 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 8
                  THEN
                     L_DED_AMT8 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 9
                  THEN
                     L_DED_AMT9 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 10
                  THEN
                     L_DED_AMT10 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 11
                  THEN
                     L_DED_AMT11 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 12
                  THEN
                     L_DED_AMT12 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 13
                  THEN
                     L_DED_AMT13 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 14
                  THEN
                     L_DED_AMT14 := L_DEFAULT_DED;
                  ELSIF L_D_SEQ = 15
                  THEN
                     L_DED_AMT15 := L_DEFAULT_DED;
                  END IF;
               END IF;

               IF L_S_SEQ <> 0
               THEN
                  IF L_S_SEQ = 1
                  THEN
                     L_DED_AMT1 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 2
                  THEN
                     L_DED_AMT2 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 3
                  THEN
                     L_DED_AMT3 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 4
                  THEN
                     L_DED_AMT4 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 5
                  THEN
                     L_DED_AMT5 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 6
                  THEN
                     L_DED_AMT6 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 7
                  THEN
                     L_DED_AMT7 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 8
                  THEN
                     L_DED_AMT8 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 9
                  THEN
                     L_DED_AMT9 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 10
                  THEN
                     L_DED_AMT10 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 11
                  THEN
                     L_DED_AMT11 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 12
                  THEN
                     L_DED_AMT12 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 13
                  THEN
                     L_DED_AMT13 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 14
                  THEN
                     L_DED_AMT14 := L_DEFAULT_SEP;
                  ELSIF L_S_SEQ = 15
                  THEN
                     L_DED_AMT15 := L_DEFAULT_SEP;
                  END IF;
               END IF;

               IF L_P_SEQ <> 0
               THEN
                  IF L_P_SEQ = 1
                  THEN
                     L_DED_AMT1 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 2
                  THEN
                     L_DED_AMT2 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 3
                  THEN
                     L_DED_AMT3 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 4
                  THEN
                     L_DED_AMT4 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 5
                  THEN
                     L_DED_AMT5 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 6
                  THEN
                     L_DED_AMT6 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 7
                  THEN
                     L_DED_AMT7 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 8
                  THEN
                     L_DED_AMT8 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 9
                  THEN
                     L_DED_AMT9 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 10
                  THEN
                     L_DED_AMT10 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 11
                  THEN
                     L_DED_AMT11 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 12
                  THEN
                     L_DED_AMT12 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 13
                  THEN
                     L_DED_AMT13 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 14
                  THEN
                     L_DED_AMT14 := L_DEFAULT_PER;
                  ELSIF L_P_SEQ = 15
                  THEN
                     L_DED_AMT15 := L_DEFAULT_PER;
                  END IF;
               END IF;
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  FND_FILE.PUT_LINE (1, ' ');
                  FND_FILE.PUT_LINE
                     (1,
                         'ERROR FETCHING THE ELEMENT INPUT VALUE AMOUNT FOR ELEMENT '
                      || TRIM (CR.DEDUCTION_CODE)
                     );
                  ERRBUF :=
                        'ERROR FETCHING THE ELEMENT INPUT VALUE  AMOUNT FOR ELEMENT '
                     || SQLERRM;
                  RETCODE := 1;
                  GOTO END_OF_LOOP;
            END;

            --
            --
            FND_FILE.PUT_LINE (DEBUG_ON, 'CREATE THE PAY BATCH LINE');

            BEGIN
               --
               PAY_BATCH_ELEMENT_ENTRY_API.CREATE_BATCH_LINE
                  (P_SESSION_DATE               => SYSDATE,
                   P_BATCH_ID                   => L_BATCH_ID,
                   P_ASSIGNMENT_ID              => L_ASSIGNMENT_ID,
                   P_ASSIGNMENT_NUMBER          => L_ASSIGNMENT_NUMBER,
                   P_EFFECTIVE_DATE             => CR.EFFECTIVE_DATE,
                   P_ELEMENT_NAME               => CR.DEDUCTION_CODE,
                   P_ENTRY_TYPE                 => 'E',
                   P_VALUE_1                    => L_DED_AMT1,
                   P_VALUE_2                    => L_DED_AMT2,
                   P_VALUE_3                    => L_DED_AMT3,
                   P_VALUE_4                    => L_DED_AMT4,
                   P_VALUE_5                    => L_DED_AMT5,
                   P_VALUE_6                    => L_DED_AMT6,
                   P_VALUE_7                    => L_DED_AMT7,
                   P_VALUE_8                    => L_DED_AMT8,
                   P_VALUE_9                    => L_DED_AMT9,
                   P_VALUE_10                   => L_DED_AMT10,
                   P_VALUE_11                   => L_DED_AMT11,
                   P_VALUE_12                   => L_DED_AMT12,
                   P_VALUE_13                   => L_DED_AMT13,
                   P_VALUE_14                   => L_DED_AMT14,
                   P_VALUE_15                   => L_DED_AMT15,
                   P_BATCH_LINE_ID              => L_BATCH_LINE_ID,
                   P_OBJECT_VERSION_NUMBER      => L_OBJECT_VERSION_NUMBER
                  );
               --
               FND_FILE.PUT_LINE (DEBUG_ON,
                                     'BATCH LINE CREATED SUCCESSFULLY: '
                                  || TO_CHAR (L_BATCH_LINE_ID)
                                 );
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  FND_FILE.PUT_LINE (1, ' ');
                  FND_FILE.PUT_LINE (1,
                                     'ERROR CREATING THE BATCH LINE'
                                     || SQLERRM
                                    );
                  ERRBUF := 'ERROR CREATING THE BATCH LINE ' || SQLERRM;
                  RETCODE := 1;
                  GOTO END_OF_LOOP;
            END;
         END IF;                                            --DEDUCTION AMOUNT

                      --
         --
         <<END_OF_LOOP>>
         BEGIN
            IF RETCODE = 0
            THEN
               --
               FND_FILE.PUT_LINE (DEBUG_ON,
                                  'UPDATING THE SOURCE RECORD WITH SUCCESS'
                                 );
               L_SUCCESS_COUNT := L_SUCCESS_COUNT + 1;

               --
               UPDATE XXARG_MERCER_INBOUND_HSA_STAGE
                  SET PROCESS_FLAG = 'Y',
                      PROCESS_MESSAGE = NULL,
                      LAST_UPDATE_DATE = SYSDATE,
                      LAST_UPDATED_BY = FND_GLOBAL.USER_ID,
                      LAST_UPDATE_LOGIN = FND_GLOBAL.LOGIN_ID,
                      REQUEST_ID = FND_GLOBAL.CONC_REQUEST_ID,
                      PROGRAM_ID = FND_GLOBAL.CONC_PROGRAM_ID,
                      PBH_BATCH_ID = L_BATCH_ID,
                      PBL_BATCH_LINE_ID = L_BATCH_LINE_ID
                WHERE CURRENT OF SRC_RECS;
            ELSE
               L_ERROR_COUNT := L_ERROR_COUNT + 1;

               UPDATE XXARG_MERCER_INBOUND_HSA_STAGE
                  SET PROCESS_FLAG = 'E',
                      PROCESS_MESSAGE = SUBSTR(ERRBUF, 1, 250), -- only post the first 250 characters of error message (CDCummins, 27-MAY-2011)
                      LAST_UPDATE_DATE = SYSDATE,
                      LAST_UPDATED_BY = FND_GLOBAL.USER_ID,
                      LAST_UPDATE_LOGIN = FND_GLOBAL.LOGIN_ID,
                      REQUEST_ID = FND_GLOBAL.CONC_REQUEST_ID,
                      PROGRAM_ID = FND_GLOBAL.CONC_PROGRAM_ID,
                      PBH_BATCH_ID = NULL,
                      PBL_BATCH_LINE_ID = NULL
                WHERE CURRENT OF SRC_RECS;
            END IF;
         END;
      END LOOP;

      BEGIN
         IF P_OPEN_ENROLLMENT_FLAG = 'Y'
         THEN
            FND_FILE.PUT_LINE (2, 'HSA BATCH ID CREATED: ' || L_HSA_BATCH_ID);
            FND_FILE.PUT_LINE
               (2,
                '     RUN THIS CONCURRENT PROGRAM:  XXARG HR HSA SPECIAL BALANCE ADJUST '
               );
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            FND_FILE.PUT_LINE (1, 'ERROR IN THE CALLING XXARG_HR_HSA_PKG  ');
      END;

      --
      FND_FILE.PUT_LINE (DEBUG_ON, 'ALL SOURCE RECORDS PROCESSED.');

      --
      IF L_SUCCESS_COUNT = 0
      THEN
         --
         FND_FILE.PUT_LINE
               (1,
                'NO BATCH LINES CREATED.  HENCE DELETE THE BATCH HEADER NOW.'
               );

         --
         BEGIN
            DELETE FROM PAY_BATCH_HEADERS
                  WHERE BATCH_ID = L_BATCH_ID;
         EXCEPTION
            WHEN OTHERS
            THEN
               FND_FILE.PUT_LINE (1, ' ');
               FND_FILE.PUT_LINE (1, 'ERROR! REMOVING THE PAY BATCH.');
               FND_FILE.PUT_LINE (1, 'ERRMSG: ' || SQLERRM);
               FND_FILE.PUT_LINE (1,
                                     'PLEASE DELETE THE BATCH '
                                  || L_BATCH_NAME
                                  || ' MANUALLY.'
                                 );
         END;
      --
      END IF;

      --
      FND_FILE.PUT_LINE (DEBUG_ON,
                         'PRINTING STATISTICS, PERFORMING COMMIT AND EXITING.'
                        );
      --
      COMMIT;
      --
      --
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE (1, '**********  PROGRAM STATS  **********');
      FND_FILE.PUT_LINE (1, ' ');

      IF L_SUCCESS_COUNT > 0
      THEN
         FND_FILE.PUT_LINE (1, 'BEE BATCH NAME CREATED: ' || L_BATCH_NAME);
         FND_FILE.PUT_LINE (2, 'BEE HB BATCH NAME CREATED: ' || L_BATCH_NAME);
      END IF;

      FND_FILE.PUT_LINE (1,
                         'SUCCESS RECORDS:   ' || TO_CHAR (L_SUCCESS_COUNT));
      FND_FILE.PUT_LINE (1, 'ERROR RECORDS:     ' || TO_CHAR (L_ERROR_COUNT));
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE (1,
                            'TOTAL SOURCE RECORDS PROCESSED: '
                         || TO_CHAR (L_COUNT)
                        );
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE (1, '************  END STATS  ************');
      FND_FILE.PUT_LINE (1, ' ');
      FND_FILE.PUT_LINE (1, ' ');

      --
      IF L_ERROR_COUNT > 0
      THEN
         GENERATE_HB_ERROR_REPORT;
         RETCODE := 1;
      ELSE
         RETCODE := 0;
      END IF;

      GENERATE_HB_END_REPORT;
   --
   EXCEPTION
      WHEN G_END_PROGRAM
      THEN
         RETCODE := 2;
         FND_FILE.PUT_LINE (1, ' ');
         FND_FILE.PUT_LINE (1,
                            'ERROR!! PROCESSING THE MERCER INBOUND HB FILE.'
                           );
         FND_FILE.PUT_LINE (1, 'ERRMSG: ' || ERRBUF);
         ROLLBACK;
      WHEN OTHERS
      THEN
         ERRBUF := SQLERRM;
         RETCODE := 2;
         ROLLBACK;
   END PROCESS_HSA_FILE;

----
---- ENDED HERE - LAKSHA
----

   PROCEDURE stage_dc_data (
      p_rerun_flag   IN              VARCHAR2,
      p_move_flag    IN              VARCHAR2,
      errbuf         IN OUT NOCOPY   VARCHAR2,
      retcode        IN OUT NOCOPY   NUMBER
   )
   IS
      l_status   NUMBER;
--
   BEGIN
      --
      IF p_rerun_flag = 'N'
      THEN
         IF p_move_flag = 'Y'
         THEN
            BEGIN
               l_status := 0;
               l_status := file_transfer ('Mercer_DC.dat', 'Mercer_DC.dat');

               IF l_status = 1
               THEN
                  errbuf := 'Error in the FTP Progrom ' || SQLERRM;
                  retcode := 2;
                  RAISE g_end_program;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  errbuf := 'Error in the FTP Progrom ' || SQLERRM;
                  retcode := 2;
                  RAISE g_end_program;
            END;
         END IF;

         --
         fnd_file.put_line (debug_on,
                            'cleaningup the staging tables after previous run'
                           );
         fnd_file.put_line
            (debug_on,
             'changing all un-processed recs from N to CN and error records from E to CE'
            );

         --
         --
         BEGIN
            UPDATE xxarg_mercer_dc001_stage
               SET process_flag = 'C' || NVL (process_flag, 'N')
             WHERE NVL (process_flag, 'N') IN ('N', 'E');
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                  (1,
                   'Error updating the DC001 data stage table during clean up..'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         BEGIN
            UPDATE xxarg_mercer_dc004_stage
               SET process_flag = 'C' || NVL (process_flag, 'N')
             WHERE NVL (process_flag, 'N') IN ('N', 'E');
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                  (1,
                   'Error updating the DC004 data stage table during clean up..'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         BEGIN
            UPDATE xxarg_mercer_dc005_stage
               SET process_flag = 'C' || NVL (process_flag, 'N')
             WHERE NVL (process_flag, 'N') IN ('N', 'E');
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                  (1,
                   'Error updating the DC005 data stage table during clean up..'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         --
         fnd_file.put_line (debug_on,
                            'now load the staging table from the data file'
                           );
         fnd_file.put_line (debug_on, 'loading the DC001 staging table..');

         --
         BEGIN
            INSERT INTO xxarg_mercer_dc001_stage
                        (employee_ssn, transaction_type, SOURCE, new_percent,
                         entry_date, employee_number, LOCATION, status_code,
                         creation_date, created_by, last_update_date,
                         last_updated_by, last_update_login, program_id,
                         request_id, process_flag, process_message,
                         pbh_batch_id, pbl_batch_line_id)
               SELECT employee_ssn, transaction_type, SOURCE, new_percent,
                      CASE
                         WHEN TO_DATE (entry_date, 'MMDDYYYY') >= SYSDATE
                            THEN entry_date
                         ELSE TO_CHAR (SYSDATE, 'MMDDYYYY')
                      END,
                      employee_number, LOCATION, status_code, SYSDATE,
                      fnd_global.user_id, SYSDATE, fnd_global.user_id,
                      fnd_global.login_id, fnd_global.conc_program_id,
                      fnd_global.conc_request_id, 'N', NULL, NULL, NULL
                 FROM xxarg_mercer_dc001_file;
         --
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                  (1,
                   'Error inserting data from source file into DC001 staging table'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         fnd_file.put_line (debug_on, 'loading the DC004 staging table..');

         --
         BEGIN
            INSERT INTO xxarg_mercer_dc004_stage
                        (employee_ssn, transaction_type, employee_number,
                         loan_bucket, loan_status, loan_status_date,
                         loan_balance, loan_type, LOCATION, creation_date,
                         created_by, last_update_date, last_updated_by,
                         last_update_login, program_id, request_id,
                         process_flag, process_message, pbh_batch_id,
                         pbl_batch_line_id)
               SELECT employee_ssn, transaction_type, employee_number,
                      loan_bucket, loan_status, loan_status_date,
                      loan_balance, loan_type, LOCATION, SYSDATE,
                      fnd_global.user_id, SYSDATE, fnd_global.user_id,
                      fnd_global.login_id, fnd_global.conc_program_id,
                      fnd_global.conc_request_id, 'N', NULL, NULL, NULL
                 FROM xxarg_mercer_dc004_file;
         --
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                  (1,
                   'Error inserting data from source file into DC004 staging table'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         fnd_file.put_line (debug_on, 'loading the DC005 staging table..');

         --
         BEGIN
            INSERT INTO xxarg_mercer_dc005_stage
                        (employee_ssn, transaction_type, loan_number,
                         loan_type, loan_amount, interest_rate,
                         repayment_amount, loan_duration, loan_payment_date,
                         reamortization_ind, employee_number, LOCATION,
                         repayment_frequency, creation_date, created_by,
                         last_update_date, last_updated_by,
                         last_update_login, program_id, request_id,
                         process_flag, process_message, pbh_batch_id,
                         pbl_batch_line_id)
               SELECT employee_ssn, transaction_type, loan_number, loan_type,
                      loan_amount, interest_rate, repayment_amount,
                      loan_duration,
                      CASE
                         WHEN TO_DATE (loan_payment_date, 'MMDDYYYY') >=
                                                                      SYSDATE
                            THEN loan_payment_date
                         ELSE TO_CHAR (SYSDATE, 'MMDDYYYY')
                      END,
                      reamortization_ind, employee_number, LOCATION,
                      repayment_frequency, SYSDATE, fnd_global.user_id,
                      SYSDATE, fnd_global.user_id, fnd_global.login_id,
                      fnd_global.conc_program_id, fnd_global.conc_request_id,
                      'N', NULL, NULL, NULL
                 FROM xxarg_mercer_dc005_file;
         --
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                  (1,
                   'Error inserting data from source file into DC005 staging table'
                  );
               errbuf := SQLERRM;
               RAISE g_end_program;
         END;

         --
         --
         fnd_file.put_line (debug_on,
                            'Performing commit and returning success.'
                           );
         COMMIT;
         errbuf := NULL;
         retcode := 0;
      --
      END IF;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         errbuf := SQLERRM;
         retcode := 2;
   END stage_dc_data;

--
--
   PROCEDURE process_dc005_recs (
      l_count                 IN OUT NOCOPY   NUMBER,
      l_success_dc005_count   IN OUT NOCOPY   NUMBER,
      l_error_dc005_count     IN OUT NOCOPY   NUMBER,
      l_batch_id              IN              NUMBER,
      errbuf                  IN OUT NOCOPY   VARCHAR2,
      retcode                 IN OUT NOCOPY   NUMBER
   )
   IS
--
      l_full_name               VARCHAR2 (100);
      l_person_id               NUMBER;
      l_assignment_id           NUMBER;
      l_assignment_number       VARCHAR2 (30);
      l_object_version_number   NUMBER         := 0;
      l_batch_line_id           NUMBER         := 0;
--
      l_loan1_seq               NUMBER         := 0;
      l_loan1_to_seq            NUMBER         := 0;
      l_loan1_element_type_id   NUMBER         := 0;
      l_loan1_element_name      VARCHAR2 (30)  := NULL;
--
      l_loan2_seq               NUMBER         := 0;
      l_loan2_to_seq            NUMBER         := 0;
      l_loan2_element_type_id   NUMBER         := 0;
      l_loan2_element_name      VARCHAR2 (30)  := NULL;
--
      l_value_1                 NUMBER         := 0;
      l_value_2                 NUMBER         := 0;
      l_value_3                 NUMBER         := 0;
      l_value_4                 NUMBER         := 0;
      l_value_5                 NUMBER         := 0;
      l_value_6                 NUMBER         := 0;
      l_value_7                 NUMBER         := 0;
      l_value_8                 NUMBER         := 0;
      l_value_9                 NUMBER         := 0;
      l_value_10                NUMBER         := 0;

--
--
      CURSOR dc005
      IS
         SELECT     *
               FROM xxarg_mercer_dc005_stage A
              WHERE NVL (A.process_flag, 'N') IN ('N', 'E')
         FOR UPDATE;

      CURSOR c1 (p_element_type_id NUMBER)
      IS
         SELECT   input_value_id, NAME, display_sequence
             FROM pay_input_values_f piv
            WHERE piv.element_type_id = p_element_type_id
         -- AND UPPER(NAME) != 'PAY VALUE'
         ORDER BY display_sequence, NAME;

      v_count                   NUMBER;
--
   BEGIN
      --
      fnd_file.put_line (debug_on, 'Inside process_dc005_recs procedure...');
      fnd_file.put_line (debug_on, 'first fetch the Loan elements..');

      --
      BEGIN
         SELECT pivf1.display_sequence, pivf1to.display_sequence,
                petf1.element_type_id, petf1.element_name
                                                         --
         ,
                pivf2.display_sequence, pivf2to.display_sequence,
                petf2.element_type_id, petf2.element_name
           INTO l_loan1_seq, l_loan1_to_seq,
                l_loan1_element_type_id, l_loan1_element_name
                                                             --
         ,
                l_loan2_seq, l_loan2_to_seq,
                l_loan2_element_type_id, l_loan2_element_name
           FROM pay_input_values_f pivf1,
                pay_element_types_f petf1,
                pay_input_values_f pivf1to,
                pay_input_values_f pivf2,
                pay_element_types_f petf2,
                pay_input_values_f pivf2to
          WHERE pivf1.element_type_id = petf1.element_type_id
            AND TRUNC (SYSDATE) BETWEEN TRUNC (petf1.effective_start_date)
                                    AND TRUNC (petf1.effective_end_date)
            AND TRUNC (SYSDATE) BETWEEN TRUNC (pivf1.effective_start_date)
                                    AND TRUNC (pivf1.effective_end_date)
            AND UPPER (petf1.element_name) = 'WAG 401K LOAN 1'
            AND UPPER (pivf1.NAME) = 'AMOUNT'
            AND pivf1to.element_type_id = petf1.element_type_id
            AND TRUNC (SYSDATE) BETWEEN TRUNC (pivf1to.effective_start_date)
                                    AND TRUNC (pivf1to.effective_end_date)
            AND UPPER (pivf1to.NAME) = 'TOTAL OWED'
            --
            AND pivf2.element_type_id = petf2.element_type_id
            AND TRUNC (SYSDATE) BETWEEN TRUNC (petf2.effective_start_date)
                                    AND TRUNC (petf2.effective_end_date)
            AND TRUNC (SYSDATE) BETWEEN TRUNC (pivf2.effective_start_date)
                                    AND TRUNC (pivf2.effective_end_date)
            AND UPPER (petf2.element_name) = 'WAG 401K LOAN 2'
            AND UPPER (pivf2.NAME) = 'AMOUNT'
            AND pivf2to.element_type_id = petf2.element_type_id
            AND TRUNC (SYSDATE) BETWEEN TRUNC (pivf2to.effective_start_date)
                                    AND TRUNC (pivf2to.effective_end_date)
            AND UPPER (pivf2to.NAME) = 'TOTAL OWED';
      EXCEPTION
         WHEN OTHERS
         THEN
            fnd_file.put_line (1, ' ');
            fnd_file.put_line (1, 'Error fetching the Element details');
            fnd_file.put_line
                            (1,
                             'Element Names: WAG 401K LOAN 1/WAG 401K LOAN 2'
                            );
            fnd_file.put_line (1, 'Input Value Name: Amount');
            errbuf := 'Error fetching the Element details ' || SQLERRM;
            RAISE g_end_program;
      END;

      --
      --
      v_count := 0;

      FOR c1_rec IN c1 (l_loan1_element_type_id)
      LOOP
         v_count := v_count + 1;

         IF (UPPER (c1_rec.NAME) = 'AMOUNT')
         THEN
            l_loan1_seq := v_count;
         ELSIF (UPPER (c1_rec.NAME) = 'TOTAL OWED')
         THEN
            l_loan1_to_seq := v_count;
         END IF;
      END LOOP;

      v_count := 0;

      FOR c1_rec IN c1 (l_loan2_element_type_id)
      LOOP
         v_count := v_count + 1;

         IF (UPPER (c1_rec.NAME) = 'AMOUNT')
         THEN
            l_loan2_seq := v_count;
         ELSIF (UPPER (c1_rec.NAME) = 'TOTAL OWED')
         THEN
            l_loan2_to_seq := v_count;
         END IF;
      END LOOP;

      fnd_file.put_line
                       (debug_on,
                        'Loop thru the source recs and create the BEE lines..'
                       );

      --
      FOR cr005 IN dc005
      LOOP
         --
         errbuf := NULL;
         retcode := 0;
         l_assignment_id := 0;
         l_assignment_number := NULL;
         --
         l_value_1 := NULL;
         l_value_2 := NULL;
         l_value_3 := NULL;
         l_value_4 := NULL;
         l_value_5 := NULL;
         l_value_6 := NULL;
         l_value_7 := NULL;
         l_value_8 := NULL;
         l_value_9 := NULL;
         l_value_10 := NULL;
         --
         fnd_file.put_line (debug_on, ' ');
         fnd_file.put_line (debug_on, 'Employee SSN: ' || cr005.employee_ssn);
         fnd_file.put_line (debug_on,
                            'Employee ID: ' || cr005.employee_number
                           );
         fnd_file.put_line (debug_on, 'Fetch the employee details.. ');

         --
         BEGIN
            --
            SELECT DISTINCT papf.full_name, papf.person_id,
                            paaf.assignment_id, paaf.assignment_number
                       INTO l_full_name, l_person_id,
                            l_assignment_id, l_assignment_number
                       FROM per_all_people_f papf, per_all_assignments_f paaf
                      WHERE LTRIM (papf.employee_number, 0) =
                                       LTRIM (TRIM (cr005.employee_number), 0)
                        AND xxarg_unfrmt_natl_id_fnc (papf.national_identifier) =
                                 xxarg_unfrmt_natl_id_fnc (cr005.employee_ssn)
                        AND paaf.person_id = papf.person_id
                        AND TRUNC (TO_DATE (cr005.loan_payment_date,
                                            'MMDDYYYY'
                                           )
                                  ) BETWEEN TRUNC (papf.effective_start_date)
                                        AND TRUNC (papf.effective_end_date)
                        AND TRUNC (TO_DATE (cr005.loan_payment_date,
                                            'MMDDYYYY'
                                           )
                                  ) BETWEEN TRUNC (paaf.effective_start_date)
                                        AND TRUNC (paaf.effective_end_date)
                        AND paaf.primary_flag = 'Y';

            --
            fnd_file.put_line (debug_on, 'Employee Name: ' || l_full_name);
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                       (1,
                        'Error fetching the employee details record type 005'
                       );
               errbuf :=
                     'Error fetching the employee details record type 005 '
                  || SQLERRM;
               retcode := 1;
               GOTO end_of_loop_dc005;
         END;

         --
         --
         IF TO_NUMBER (cr005.loan_number) = 1
         THEN
            fnd_file.put_line (debug_on, 'Processing Loan1 element.');

            --
            IF l_loan1_seq = 1
            THEN
               l_value_1 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 2
            THEN
               l_value_2 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 3
            THEN
               l_value_3 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 4
            THEN
               l_value_4 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 5
            THEN
               l_value_5 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 6
            THEN
               l_value_6 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 7
            THEN
               l_value_7 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 8
            THEN
               l_value_8 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 9
            THEN
               l_value_9 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan1_seq = 10
            THEN
               l_value_10 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            END IF;

            --
            --
            -- /*Narsi(Eis) changed bleow values to null to stop the loading of total owed 24-FEB-2010*/

            IF l_loan1_to_seq = 1
            THEN
               l_value_1 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 2
            THEN
               l_value_2 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 3
            THEN
               l_value_3 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 4
            THEN
               l_value_4 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 5
            THEN
               l_value_5 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 6
            THEN
               l_value_6 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 7
            THEN
               l_value_7 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 8
            THEN
               l_value_8 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 9
            THEN
               l_value_9 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan1_to_seq = 10
            THEN
               l_value_10 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            END IF;
         --
         ELSE
            fnd_file.put_line (debug_on, 'Processing Loan2 element.');

            --
            IF l_loan2_seq = 1
            THEN
               l_value_1 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 2
            THEN
               l_value_2 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 3
            THEN
               l_value_3 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 4
            THEN
               l_value_4 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 5
            THEN
               l_value_5 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 6
            THEN
               l_value_6 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 7
            THEN
               l_value_7 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 8
            THEN
               l_value_8 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 9
            THEN
               l_value_9 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            ELSIF l_loan2_seq = 10
            THEN
               l_value_10 := TO_NUMBER (TRIM (cr005.repayment_amount)) / 100;
            END IF;

            --
            --
            -- /*Narsi(Eis) changed bleow values to null to stop the loading of total owed 24-FEB-2010*/

            IF l_loan2_to_seq = 1
            THEN
               l_value_1 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 2
            THEN
               l_value_2 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 3
            THEN
               l_value_3 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 4
            THEN
               l_value_4 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 5
            THEN
               l_value_5 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 6
            THEN
               l_value_6 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 7
            THEN
               l_value_7 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 8
            THEN
               l_value_8 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 9
            THEN
               l_value_9 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            ELSIF l_loan2_to_seq = 10
            THEN
               l_value_10 := null; --TO_NUMBER (TRIM (cr005.loan_amount)) / 100;
            END IF;
         --
         END IF;

         --
         fnd_file.put_line (debug_on, 'Now create the Pay Batch Line');

         --
         BEGIN
            pay_batch_element_entry_api.create_batch_line
               (p_session_date               => SYSDATE,
                p_batch_id                   => l_batch_id,
                p_assignment_id              => l_assignment_id,
                p_assignment_number          => l_assignment_number,
                p_effective_date             => TO_DATE
                                                     (cr005.loan_payment_date,
                                                      'MMDDYYYY'
                                                     ),
                p_element_name               => CASE
                   WHEN TO_NUMBER (cr005.loan_number) = 1
                      THEN l_loan1_element_name
                   ELSE l_loan2_element_name
                END,
                p_entry_type                 => 'E'
                                            -- THIS IS A REGULAR ELEMENT ENTRY
                                                   ,
                p_value_1                    => l_value_1,
                p_value_2                    => l_value_2,
                p_value_3                    => l_value_3,
                p_value_4                    => l_value_4,
                p_value_5                    => l_value_5,
                p_value_6                    => l_value_6,
                p_value_7                    => l_value_7,
                p_value_8                    => l_value_8,
                p_value_9                    => l_value_9,
                p_value_10                   => l_value_10,
                p_batch_line_id              => l_batch_line_id,
                p_object_version_number      => l_object_version_number
               );
            --
            fnd_file.put_line (debug_on,
                                  'Batch Line created successfully: '
                               || TO_CHAR (l_batch_line_id)
                              );
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line (1, 'Error creating the Batch Line');
               errbuf := 'Error creating the Batch Line ' || SQLERRM;
               retcode := 1;
               GOTO end_of_loop_dc005;
         END;

         --
         <<end_of_loop_dc005>>
         BEGIN
            IF retcode = 0
            THEN
               --
               fnd_file.put_line (debug_on,
                                  'Updating the source record with success'
                                 );
               l_success_dc005_count := l_success_dc005_count + 1;

               --
               UPDATE xxarg_mercer_dc005_stage
                  SET process_flag = 'Y',
                      process_message = NULL,
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = l_batch_id,
                      pbl_batch_line_id = l_batch_line_id
                WHERE CURRENT OF dc005;
            ELSE
               l_error_dc005_count := l_error_dc005_count + 1;

               UPDATE xxarg_mercer_dc005_stage
                  SET process_flag = 'E',
                      process_message = SUBSTR(errbuf, 1, 250), -- only post the first 250 characters of error message (CDCummins, 27-MAY-2011)
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = NULL,
                      pbl_batch_line_id = NULL
                WHERE CURRENT OF dc005;
            END IF;
         END;
      END LOOP;

      --
      --
      errbuf := NULL;
      retcode := 0;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         errbuf := SQLERRM;
         retcode := 2;
   END process_dc005_recs;

--
--
   PROCEDURE process_dc004_recs (
      l_count                 IN OUT NOCOPY   NUMBER,
      l_success_dc004_count   IN OUT NOCOPY   NUMBER,
      l_error_dc004_count     IN OUT NOCOPY   NUMBER,
      l_batch_id              IN              NUMBER,
      errbuf                  IN OUT NOCOPY   VARCHAR2,
      retcode                 IN OUT NOCOPY   NUMBER
   )
   IS
--
      l_element_entry_id   NUMBER  := 0;
      l_ovn                NUMBER  := 0;
      l_eff_start_date     DATE    := NULL;
      l_eff_end_date       DATE    := NULL;
      l_warning            BOOLEAN;

--
      CURSOR dc004
      IS
         SELECT     *
               FROM xxarg_mercer_dc004_stage A
              WHERE NVL (A.process_flag, 'N') IN ('N', 'E')
         FOR UPDATE;
--
   BEGIN
      fnd_file.put_line (debug_on, 'Inside process_dc004_recs procedure...');

      --
      FOR cr004 IN dc004
      LOOP
         errbuf := NULL;
         retcode := 0;
         --
         fnd_file.put_line (debug_on, ' ');
         fnd_file.put_line (debug_on,
                               'Fetching the element entry ID for: '
                            || TRIM (cr004.employee_number)
                           );

         --
         BEGIN
            SELECT peef.element_entry_id, peef.object_version_number
              INTO l_element_entry_id, l_ovn
              FROM pay_element_entries_f peef,
                   per_all_people_f papf,
                   per_all_assignments_f paaf,
                   pay_element_types_f petf
             WHERE paaf.person_id = papf.person_id
               AND TRUNC (TO_DATE (cr004.loan_status_date, 'MMDDYYYY'))
                      BETWEEN TRUNC (papf.effective_start_date)
                          AND TRUNC (papf.effective_end_date)
               AND TRUNC (TO_DATE (cr004.loan_status_date, 'MMDDYYYY'))
                      BETWEEN TRUNC (paaf.effective_start_date)
                          AND TRUNC (paaf.effective_end_date)
               AND paaf.primary_flag = 'Y'
               AND paaf.assignment_type = 'E'
               AND peef.assignment_id = paaf.assignment_id
               AND TRUNC (TO_DATE (cr004.loan_status_date, 'MMDDYYYY'))
                      BETWEEN TRUNC (peef.effective_start_date)
                          AND TRUNC (peef.effective_end_date)
               AND petf.element_type_id = peef.element_type_id
               AND TRUNC (TO_DATE (cr004.loan_status_date, 'MMDDYYYY'))
                      BETWEEN TRUNC (petf.effective_start_date)
                          AND TRUNC (petf.effective_end_date)
               --
               AND LTRIM (papf.employee_number, 0) =
                                       LTRIM (TRIM (cr004.employee_number), 0)
               AND xxarg_unfrmt_natl_id_fnc (papf.national_identifier) =
                                 xxarg_unfrmt_natl_id_fnc (cr004.employee_ssn)
               AND UPPER (petf.element_name) =
                      DECODE (TO_NUMBER (cr004.loan_bucket),
                              1, 'WAG 401K LOAN 1',
                              2, 'WAG 401K LOAN 2',
                              NULL
                             );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               fnd_file.put_line (debug_on, ' ');
               fnd_file.put_line
                      (debug_on,
                       'Loan Element entry does not exist for this employee.'
                      );
               fnd_file.put_line (debug_on,
                                  'Emp#: ' || TRIM (cr004.employee_number)
                                 );
               fnd_file.put_line (debug_on,
                                     'Loan Number: '
                                  || TO_CHAR (TO_NUMBER (cr004.loan_bucket))
                                 );
               errbuf :=
                     'Loan Number: '
                  || TO_CHAR (TO_NUMBER (cr004.loan_bucket))
                  || ' Element entry does not exist for this employee.';
               retcode := 1;
               GOTO end_of_loop_dc004;
            WHEN OTHERS
            THEN
               fnd_file.put_line
                             (1,
                              'ERROR! Fetching the element entry details... '
                             );
               fnd_file.put_line (1, 'Emp#: ' || TRIM (cr004.employee_number));
               fnd_file.put_line (1,
                                     'Loan Number: '
                                  || TO_CHAR (TO_NUMBER (cr004.loan_bucket))
                                 );
               errbuf :=
                      'ERROR! Fetching the element entry details. ' || SQLERRM;
               retcode := 2;
               RAISE g_end_program;
         END;

         --
         --
         fnd_file.put_line
                         (debug_on,
                          'now calling the API to delete the element entry.. '
                         );

         --
         BEGIN
            pay_element_entry_api.delete_element_entry
                (p_datetrack_delete_mode      => 'DELETE',
                 p_effective_date             => TRUNC
                                                    (TO_DATE
                                                        (cr004.loan_status_date,
                                                         'MMDDYYYY'
                                                        )
                                                    ),
                 p_element_entry_id           => l_element_entry_id,
                 p_object_version_number      => l_ovn,
                 p_effective_start_date       => l_eff_start_date,
                 p_effective_end_date         => l_eff_end_date,
                 p_delete_warning             => l_warning
                );
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line
                             (debug_on,
                              'ERROR! calling the delete_element_entry API..'
                             );
               fnd_file.put_line (debug_on, 'ERRMSG: ' || SQLERRM);
               errbuf :=
                    'ERROR! calling the delete_element_entry API..' || SQLERRM;
               retcode := 1;
               GOTO end_of_loop_dc004;
         END;

         --
         <<end_of_loop_dc004>>
         BEGIN
            IF retcode = 0
            THEN
               --
               fnd_file.put_line (debug_on,
                                  'Updating the source record with success'
                                 );
               l_success_dc004_count := l_success_dc004_count + 1;

               --
               UPDATE xxarg_mercer_dc004_stage
                  SET process_flag = 'Y',
                      process_message = NULL,
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = l_batch_id,
                      pbl_batch_line_id = NULL
                WHERE CURRENT OF dc004;
            ELSE
               fnd_file.put_line (debug_on,
                                  'Updating the source record with error'
                                 );
               l_error_dc004_count := l_error_dc004_count + 1;

               UPDATE xxarg_mercer_dc004_stage
                  SET process_flag = 'E',
                      process_message = SUBSTR(errbuf, 1, 250), -- only post the first 250 characters of error message (CDCummins, 27-MAY-2011)
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = NULL,
                      pbl_batch_line_id = NULL
                WHERE CURRENT OF dc004;
            END IF;
         END;
      END LOOP;

      --
      errbuf := NULL;
      retcode := 0;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         errbuf := SQLERRM;
         retcode := 2;
   END process_dc004_recs;

--
--
   PROCEDURE process_dc_file (
      errbuf                OUT      VARCHAR2,
      retcode               OUT      NUMBER,
      p_business_group_id   IN       NUMBER,
      p_move_flag           IN       VARCHAR2,
      p_rerun_flag          IN       VARCHAR2,
      p_debug_flag          IN       VARCHAR2
   )
   IS
--
      l_count                      NUMBER                                := 0;
      l_success_dc001_count        NUMBER                                := 0;
      l_success_dc004_count        NUMBER                                := 0;
      l_success_dc005_count        NUMBER                                := 0;
      l_error_dc001_count          NUMBER                                := 0;
      l_error_dc004_count          NUMBER                                := 0;
      l_error_dc005_count          NUMBER                                := 0;
--
      l_full_name                  VARCHAR2 (1000);
      l_person_id                  NUMBER;
      l_assignment_id              NUMBER;
      l_assignment_number          VARCHAR2 (30);
      l_batch_id                   NUMBER                                := 0;
      l_batch_name                 VARCHAR2 (30)                      := NULL;
      l_object_version_number      NUMBER                                := 0;
      l_batch_line_id              NUMBER                                := 0;
--
--
      l_401k_seq                   NUMBER                                := 0;
      l_401k_element_type_id       NUMBER                                := 0;
      l_401k_element_name          VARCHAR2 (30)                      := NULL;
      l_401k_catchup_element_type_id NUMBER                                := 0;
      l_401k_catchup_element_name    VARCHAR2 (30)                      := NULL;
--
      l_401k_elig_date             VARCHAR2 (25)                      := NULL;
--
      l_def_percent1               NUMBER                             := NULL;
      l_def_percent2               NUMBER                             := NULL;
      l_def_percent3               NUMBER                             := NULL;
      l_def_percent4               NUMBER                             := NULL;
      l_def_percent5               NUMBER                             := NULL;
      l_def_percent6               NUMBER                             := NULL;
      l_def_percent7               NUMBER                             := NULL;
      l_def_percent8               NUMBER                             := NULL;
      l_def_percent9               NUMBER                             := NULL;
      l_def_percent10              NUMBER                             := NULL;
      l_p_object_version_number    apps.per_all_people_f.object_version_number%TYPE;
      l_employee_number            apps.per_all_people_f.employee_number%TYPE;
      l_per_effective_start_date   apps.per_all_people_f.effective_start_date%TYPE;
      l_per_effective_end_date     apps.per_all_people_f.effective_end_date%TYPE;
      l_comment_id                 apps.per_all_people_f.comment_id%TYPE;
      l_name_combination_warning   BOOLEAN;
      l_assign_payroll_warning     BOOLEAN;
      l_orig_hire_warning          BOOLEAN;
      l_element_entry_id           NUMBER                                := 0;
      l_ovn                        NUMBER                                := 0;
      l_eff_start_date             DATE                               := NULL;
      l_eff_end_date               DATE                               := NULL;
      l_warning                    BOOLEAN;

--
--
      CURSOR dc001
      IS
         SELECT     *
               FROM xxarg_mercer_dc001_stage A
              WHERE NVL (A.process_flag, 'N') IN ('N', 'E')
         --    AND EMPLOYEE_NUMBER='0000002314'
         FOR UPDATE;

--
      CURSOR dc004
      IS
         SELECT     *
               FROM xxarg_mercer_dc004_stage A
              WHERE NVL (A.process_flag, 'N') IN ('N', 'E')
         FOR UPDATE;

--
      CURSOR dc005
      IS
         SELECT     *
               FROM xxarg_mercer_dc005_stage A
              WHERE NVL (A.process_flag, 'N') IN ('N', 'E')
         FOR UPDATE;
--
   BEGIN
      --
      g_debug_flag := p_debug_flag;
      --
      errbuf := NULL;
      retcode := 0;
      --
      fnd_file.put_line (1, ' ');
      fnd_file.put_line
                 (1,
                     'Mercer Defined Contributions Inbound process started: '
                  || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                 );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line
                  (debug_on,
                   'Inside package XXARG_MERCER_INBOUND_PKG.PROCESS_DC_FILE..'
                  );
      fnd_file.put_line (debug_on, 'Rerun Flag is ' || p_rerun_flag);
      fnd_file.put_line (debug_on,
                         'First clean up previous runs and stage the data...'
                        );

      --
      --
      BEGIN
         stage_dc_data (p_rerun_flag, p_move_flag, errbuf, retcode);

         --
         IF retcode <> 0
         THEN
            fnd_file.put_line (1, 'ERROR!! staging the source DC data...');
            fnd_file.put_line (1, 'ERRMSG: ' || errbuf);
            RAISE g_end_program;
         END IF;
      --
      EXCEPTION
         WHEN OTHERS
         THEN
            errbuf := SQLERRM;
            retcode := 2;
            RAISE g_end_program;
      END;

      --
      --
      fnd_file.put_line (debug_on, 'All incoming data staged successfully..');
      fnd_file.put_line (debug_on, 'First fetch the 401K Element details..');

      --
      BEGIN
         SELECT pivf.display_sequence, petf.element_type_id,
                petf.element_name
           INTO l_401k_seq, l_401k_element_type_id,
                l_401k_element_name
           FROM pay_input_values_f pivf, pay_element_types_f petf
          WHERE pivf.element_type_id = petf.element_type_id
            AND TRUNC (SYSDATE) BETWEEN TRUNC (petf.effective_start_date)
                                    AND TRUNC (petf.effective_end_date)
            AND TRUNC (SYSDATE) BETWEEN TRUNC (pivf.effective_start_date)
                                    AND TRUNC (pivf.effective_end_date)
            AND UPPER (petf.element_name) = 'WAG 401K'
            AND UPPER (pivf.NAME) = 'PERCENTAGE';
      EXCEPTION
         WHEN OTHERS
         THEN
            fnd_file.put_line (1, ' ');
            fnd_file.put_line (1, 'Error fetching the Element details');
            fnd_file.put_line (1, 'Element Name: WAG 401K');
            fnd_file.put_line (1, 'Input Value Name: Percentage');
            errbuf := 'Error fetching the Element details ' || SQLERRM;
            RAISE g_end_program;
      END;
      
      BEGIN
         SELECT petf.element_type_id,
                petf.element_name
           INTO l_401k_catchup_element_type_id,
                l_401k_catchup_element_name
           FROM pay_element_types_f petf
          WHERE TRUNC (SYSDATE) BETWEEN TRUNC (petf.effective_start_date)
                                    AND TRUNC (petf.effective_end_date)
            AND UPPER (petf.element_name) = 'WAG 401K CATCHUP';
      EXCEPTION
         WHEN OTHERS
         THEN
            fnd_file.put_line (1, ' ');
            fnd_file.put_line (1, 'Error fetching the Element details');
            fnd_file.put_line (1, 'Element Name: WAG 401K Catchup');
            errbuf := 'Error fetching the Element details ' || SQLERRM;
            RAISE g_end_program;
      END;

      --
      --
      fnd_file.put_line (debug_on, 'Now create BEE batch header.');

      --
      BEGIN
         --
         l_batch_name := 'MERCER_DC_' || TO_CHAR (SYSDATE, 'YYYYMMDD_SSSSS');
         --
         pay_batch_element_entry_api.create_batch_header
                     (p_session_date                => TRUNC (SYSDATE),
                      p_batch_name                  => l_batch_name,
                      p_business_group_id           => p_business_group_id,
                      p_action_if_exists            => 'U',
                      p_batch_source                => 'Mercer DC File',
                      p_batch_reference             => TO_CHAR
                                                          (SYSDATE,
                                                           'DD-MON-RRRR HH24:MI:SS'
                                                          ),
                      p_date_effective_changes      => 'U',
                      p_batch_id                    => l_batch_id,
                      p_object_version_number       => l_object_version_number
                     );
         --
         fnd_file.put_line (debug_on,
                               'Batch ID created successfully: '
                            || TO_CHAR (l_batch_id)
                           );
         fnd_file.put_line (debug_on, 'Batch Name: ' || l_batch_name);
         fnd_file.put_line (debug_on, ' ');
      --
      EXCEPTION
         WHEN OTHERS
         THEN
            fnd_file.put_line (1, ' ');
            fnd_file.put_line (1, 'Error creating Pay Batch Header');
            errbuf := 'Error creating Pay Batch Header ' || SQLERRM;
            RAISE g_end_program;
      END;

      --
      --
      fnd_file.put_line (debug_on, 'Processing the DC001 records now.. ');

      --
      FOR cr001 IN dc001
      LOOP
         --
         errbuf := NULL;
         retcode := 0;
         l_assignment_id := 0;
         l_assignment_number := 0;
         l_p_object_version_number := NULL;
         l_employee_number := NULL;
         l_person_id := 0;
         l_full_name := NULL;
         l_401k_elig_date := NULL;
         l_def_percent1 := NULL;
         l_def_percent2 := NULL;
         l_def_percent3 := NULL;
         l_def_percent4 := NULL;
         l_def_percent5 := NULL;
         l_def_percent6 := NULL;
         l_def_percent7 := NULL;
         l_def_percent8 := NULL;
         l_def_percent9 := NULL;
         l_def_percent10 := NULL;
         l_batch_line_id := 0;
         --
         --
         l_count := l_count + 1;
         --
         fnd_file.put_line (debug_on, ' ');
         fnd_file.put_line (debug_on, 'Employee SSN: ' || cr001.employee_ssn);
         fnd_file.put_line (debug_on,
                            'Employee ID: ' || cr001.employee_number
                           );
         fnd_file.put_line (debug_on,
                               '401K Entry Date: '
                            || TO_CHAR (TO_DATE (cr001.entry_date, 'MMDDYYYY'),
                                        'DD-MON-RRRR'
                                       )
                           );
         fnd_file.put_line (debug_on,
                               'New Percent: '
                            || TO_CHAR (TO_NUMBER (cr001.new_percent) / 100)
                            || ' %'
                           );
         fnd_file.put_line (debug_on, ' ');
         fnd_file.put_line (debug_on, 'Fetch the employee details.. ');

         --
         BEGIN
            --
            SELECT DISTINCT papf.full_name, papf.person_id, papf.attribute4,
                            paaf.assignment_id, paaf.assignment_number,
                            papf.employee_number, papf.object_version_number
                       INTO l_full_name, l_person_id, l_401k_elig_date,
                            l_assignment_id, l_assignment_number,
                            l_employee_number, l_p_object_version_number
                       FROM per_all_people_f papf, per_all_assignments_f paaf
                      WHERE LTRIM (papf.employee_number, 0) =
                                       LTRIM (TRIM (cr001.employee_number), 0)
                        AND xxarg_unfrmt_natl_id_fnc (papf.national_identifier) =
                                 xxarg_unfrmt_natl_id_fnc (cr001.employee_ssn)
                        AND paaf.person_id = papf.person_id
                        AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                               BETWEEN TRUNC (papf.effective_start_date)
                                   AND TRUNC (papf.effective_end_date)
                        AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                               BETWEEN TRUNC (paaf.effective_start_date)
                                   AND TRUNC (paaf.effective_end_date)
                        AND paaf.primary_flag = 'Y';

            --
            fnd_file.put_line (debug_on, 'Employee Name: ' || l_full_name);
         --
         EXCEPTION
            WHEN OTHERS
            THEN
               fnd_file.put_line (1, ' ');
               fnd_file.put_line
                    (1,
                        'Error fetching the employee details record type 001'
                     || SQLERRM
                    );
               errbuf :=
                     'Error fetching the employee details record type 001 '
                  || SQLERRM;
               retcode := 1;
               GOTO end_of_loop_dc001;
         END;

         --
         --
         IF l_401k_elig_date IS NULL
         THEN
            --
            fnd_file.put_line
               (debug_on,
                'Updating the 401K Eligibility Date for the employee on the person record..'
               );

            --
            BEGIN
               hr_person_api.update_us_person
                  (p_effective_date                => TRUNC
                                                         (TO_DATE
                                                             (cr001.entry_date,
                                                              'MMDDYYYY'
                                                             )
                                                         ),
                   p_datetrack_update_mode         => 'CORRECTION',
                   p_person_id                     => l_person_id,
                   p_object_version_number         => l_p_object_version_number,
                   p_employee_number               => l_employee_number,
                   p_attribute4                    => fnd_date.date_to_canonical
                                                         (TO_DATE
                                                             (cr001.entry_date,
                                                              'MMDDYYYY'
                                                             )
                                                         ),
                   p_vets100a                      => hr_api.g_varchar2,
                   p_effective_start_date          => l_per_effective_start_date,
                   p_effective_end_date            => l_per_effective_end_date,
                   p_full_name                     => l_full_name,
                   p_comment_id                    => l_comment_id,
                   p_name_combination_warning      => l_name_combination_warning,
                   p_assign_payroll_warning        => l_assign_payroll_warning,
                   p_orig_hire_warning             => l_orig_hire_warning
                  );
              --
            /*  UPDATE        PER_ALL_PEOPLE_F PAPF
              SET           PAPF.ATTRIBUTE4 = FND_DATE.DATE_TO_CANONICAL(TO_DATE(CR001.ENTRY_DATE,'MMDDYYYY'))
              WHERE         PAPF.PERSON_ID = L_PERSON_ID
                  AND       TRUNC(TO_DATE(CR001.ENTRY_DATE,'MMDDYYYY'))
                                            BETWEEN TRUNC(PAPF.EFFECTIVE_START_DATE)
                                            AND     TRUNC(PAPF.EFFECTIVE_END_DATE);
              */--
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
                  fnd_file.put_line
                     (1,
                      'Error updating the 401k Eligibility date on person table.'
                     );
                  fnd_file.put_line (1, 'Employee Full Name: ' || l_full_name);
                  fnd_file.put_line (1, 'ERRMSG: ' || SQLERRM);
                  errbuf :=
                        'Error updating the 401k Eligibility date ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop_dc001;
            END;
         --
         END IF;

         --
         --
         fnd_file.put_line
                    (debug_on,
                        'Input Value "Percentage" has a display sequence of: '
                     || TO_CHAR (l_401k_seq)
                    );
         fnd_file.put_line (debug_on,
                               'Hence pay_value_'
                            || TO_CHAR (l_401k_seq)
                            || ' will be used to populate the deferral percent'
                           );

         --
         IF TO_NUMBER (TRIM (cr001.new_percent)) / 100 = 0
         THEN
            l_element_entry_id := NULL;
            l_ovn := NULL;

            BEGIN
               SELECT peef.element_entry_id, peef.object_version_number
                 INTO l_element_entry_id, l_ovn
                 FROM per_all_people_f papf,
                      per_all_assignments_f paaf,
                      pay_element_entries_f peef,
                      pay_element_links_f pelf,
                      pay_element_types_f petf
                WHERE papf.person_id = paaf.person_id
                  AND paaf.assignment_id = peef.assignment_id
                  AND peef.element_link_id = pelf.element_link_id
                  AND pelf.element_type_id = petf.element_type_id
                  AND peef.entry_type = 'E'
                  AND UPPER (petf.element_name) = 'WAG 401K'
                  AND paaf.assignment_id = l_assignment_id
                  AND peef.effective_end_date <>
                                TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                  AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                         BETWEEN papf.effective_start_date
                             AND papf.effective_end_date
                  AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                         BETWEEN paaf.effective_start_date
                             AND paaf.effective_end_date
                  AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                         BETWEEN peef.effective_start_date
                             AND peef.effective_end_date
                  AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                         BETWEEN pelf.effective_start_date
                             AND pelf.effective_end_date
                  AND TRUNC (TO_DATE (cr001.entry_date, 'MMDDYYYY'))
                         BETWEEN petf.effective_start_date
                             AND petf.effective_end_date;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  retcode := 0;
                  GOTO end_of_loop_dc001;
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                             (1,
                              'ERROR! Fetching the element entry details... '
                             );
                  fnd_file.put_line (1,
                                     'Emp#: ' || TRIM (cr001.employee_number)
                                    );
                  errbuf :=
                      'ERROR! Fetching the element entry details. ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop_dc001;
            END;

            --
            --
            fnd_file.put_line
                         (debug_on,
                          'now calling the API to delete the element entry.. '
                         );

            --
            BEGIN
               pay_element_entry_api.delete_element_entry
                      (p_datetrack_delete_mode      => 'DELETE',
                       p_effective_date             => TRUNC
                                                          (TO_DATE
                                                              (cr001.entry_date,
                                                               'MMDDYYYY'
                                                              )
                                                          ),
                       p_element_entry_id           => l_element_entry_id,
                       p_object_version_number      => l_ovn,
                       p_effective_start_date       => l_eff_start_date,
                       p_effective_end_date         => l_eff_end_date,
                       p_delete_warning             => l_warning
                      );
               l_batch_line_id := 1;
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                             (debug_on,
                              'ERROR! calling the delete_element_entry API..'
                             );
                  fnd_file.put_line (debug_on, 'ERRMSG: ' || SQLERRM);
                  errbuf :=
                     'ERROR! calling the delete_element_entry API..'
                     || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop_dc001;
            END;
         ELSE
            BEGIN
               IF l_401k_seq = 1
               THEN
                  l_def_percent1 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 2
               THEN
                  l_def_percent2 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 3
               THEN
                  l_def_percent3 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 4
               THEN
                  l_def_percent4 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 5
               THEN
                  l_def_percent5 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 6
               THEN
                  l_def_percent6 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 7
               THEN
                  l_def_percent7 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 8
               THEN
                  l_def_percent8 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 9
               THEN
                  l_def_percent9 := TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               ELSIF l_401k_seq = 10
               THEN
                  l_def_percent10 :=
                                    TO_NUMBER (TRIM (cr001.new_percent))
                                    / 100;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
                  fnd_file.put_line
                                 (1,
                                  'Error converting the Deferral percentage.'
                                 );
                  fnd_file.put_line (1, 'Source Data: ' || cr001.new_percent);
                  errbuf :=
                       'Error converting the Deferral percentage. ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop_dc001;
            END;

            --
            fnd_file.put_line (debug_on, 'Now create the Pay Batch Line');

            --
            BEGIN
               pay_batch_element_entry_api.create_batch_line
                  (p_session_date               => SYSDATE,
                   p_batch_id                   => l_batch_id,
                   p_assignment_id              => l_assignment_id,
                   p_assignment_number          => l_assignment_number,
                   p_effective_date             => TO_DATE (cr001.entry_date,
                                                            'MMDDYYYY'
                                                           ),
                   p_element_name               => l_401k_element_name,
                   p_entry_type                 => 'E'
                                            -- THIS IS A REGULAR ELEMENT ENTRY
                                                      ,
                   p_value_1                    => l_def_percent1,
                   p_value_2                    => l_def_percent2,
                   p_value_3                    => l_def_percent3,
                   p_value_4                    => 'Yes'
                               --    L_DEF_PERCENT4    --    MODIFIED BY SATYA
                                                        ,
                   p_value_5                    => l_def_percent5,
                   p_value_6                    => l_def_percent6,
                   p_value_7                    => l_def_percent7,
                   p_value_8                    => 'No Eligible Comp Limit'
                               --    L_DEF_PERCENT8    --    MODIFIED BY SATYA
                                                                           ,
                   p_value_9                    => l_def_percent9,
                   p_value_10                   => l_def_percent10,
                   p_batch_line_id              => l_batch_line_id,
                   p_object_version_number      => l_object_version_number
                  );
               --
               fnd_file.put_line (debug_on,
                                     'Batch Line created successfully: '
                                  || TO_CHAR (l_batch_line_id)
                                 );
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
                  fnd_file.put_line (1, 'Error creating the Batch Line');
                  errbuf := 'Error creating the Batch Line ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop_dc001;
            END;
            
            BEGIN
               pay_batch_element_entry_api.create_batch_line
                  (p_session_date               => SYSDATE,
                   p_batch_id                   => l_batch_id,
                   p_assignment_id              => l_assignment_id,
                   p_assignment_number          => l_assignment_number,
                   p_effective_date             => TO_DATE (cr001.entry_date,'MMDDYYYY'),
                   p_element_name               => l_401k_catchup_element_name,
                   p_entry_type                 => 'E',
                   p_value_1                    => null,
                   p_value_2                    => null,
                   p_value_3                    => null,
                   p_value_4                    => 'Yes',
                   p_value_5                    => null,
                   p_value_6                    => null,
                   p_value_7                    => 'General Catch-Up ( Age >= 50 Years)',
                   p_value_8                    => null,
                   p_value_9                    => 'One Time Deduction',
                   p_value_10                   => 'Sequential',
                   p_batch_line_id              => l_batch_line_id,
                   p_object_version_number      => l_object_version_number
                  );

               fnd_file.put_line (debug_on,
                                     'Batch Line 401k Catchup created successfully: '
                                  || TO_CHAR (l_batch_line_id)
                                 );
            --
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (1, ' ');
                  fnd_file.put_line (1, 'Error creating the Batch Line - 401k Catchup');
                  errbuf := 'Error creating the Batch Line 401k Catchup ' || SQLERRM;
                  retcode := 1;
                  GOTO end_of_loop_dc001;
            END;
            /* commit; */ --removed 20-APR-2011 (CDCummins) - causing ORA-01002 error
         END IF;

         --
         --
         <<end_of_loop_dc001>>
         BEGIN
            IF retcode = 0
            THEN
               --
               fnd_file.put_line (debug_on,
                                  'Updating the source record with success'
                                 );
               l_success_dc001_count := l_success_dc001_count + 1;

               --
               UPDATE xxarg_mercer_dc001_stage
                  SET process_flag = 'Y',
                      process_message = NULL,
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = l_batch_id,
                      pbl_batch_line_id = l_batch_line_id
                WHERE CURRENT OF dc001;
            ELSE
               l_error_dc001_count := l_error_dc001_count + 1;

               UPDATE xxarg_mercer_dc001_stage
                  SET process_flag = 'E',
                      process_message = SUBSTR(errbuf, 1, 250), -- only post the first 250 characters of error message (CDCummins, 27-MAY-2011)
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id,
                      last_update_login = fnd_global.login_id,
                      request_id = fnd_global.conc_request_id,
                      program_id = fnd_global.conc_program_id,
                      pbh_batch_id = NULL,
                      pbl_batch_line_id = NULL
                WHERE CURRENT OF dc001;
            END IF;
         END;
      --
      END LOOP;                                           -- END OF DC001 LOOP

      --
      --
      fnd_file.put_line (debug_on, 'All dc001 records processed.');
      fnd_file.put_line (debug_on,
                         'Now process loan inactivation records (dc004)'
                        );
      --
      errbuf := NULL;
      retcode := 0;

      --
      BEGIN
         process_dc004_recs (l_count,
                             l_success_dc004_count,
                             l_error_dc004_count,
                             l_batch_id,
                             errbuf,
                             retcode
                            );

         --
         IF retcode <> 0
         THEN
            fnd_file.put_line (1, 'ERROR!! processing the dc004 recs...');
            fnd_file.put_line (1, 'ERRMSG: ' || errbuf);
            RAISE g_end_program;
         END IF;
      END;

      --
      --
      fnd_file.put_line (debug_on, 'All dc004 records processed.');
      fnd_file.put_line (debug_on,
                         'Now process loan issuance records (dc005)');
      --
      errbuf := NULL;
      retcode := 0;

      --
      BEGIN
         process_dc005_recs (l_count,
                             l_success_dc005_count,
                             l_error_dc005_count,
                             l_batch_id,
                             errbuf,
                             retcode
                            );

         --
         IF retcode <> 0
         THEN
            fnd_file.put_line (1, 'ERROR!! processing the dc005 recs...');
            fnd_file.put_line (1, 'ERRMSG: ' || errbuf);
            RAISE g_end_program;
         END IF;
      END;

      --
      --
      fnd_file.put_line (debug_on, 'All dc005 records processed.');
      fnd_file.put_line (debug_on, 'Performing commit and exiting....');
      --
      COMMIT;
      --
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, '**********  Program Stats  **********');
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, 'BEE Batch Name created: ' || l_batch_name);
      fnd_file.put_line (1,
                            'DC001 Success Records:   '
                         || TO_CHAR (l_success_dc001_count)
                        );
      fnd_file.put_line (1,
                            'DC001 Error Records:     '
                         || TO_CHAR (l_error_dc001_count)
                        );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1,
                            'DC004 Success Records:   '
                         || TO_CHAR (l_success_dc004_count)
                        );
      fnd_file.put_line (1,
                            'DC004 Error Records:     '
                         || TO_CHAR (l_error_dc004_count)
                        );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1,
                            'DC005 Success Records:   '
                         || TO_CHAR (l_success_dc005_count)
                        );
      fnd_file.put_line (1,
                            'DC005 Error Records:     '
                         || TO_CHAR (l_error_dc005_count)
                        );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1,
                            'Total Source Records Processed: '
                         || TO_CHAR (l_count)
                        );
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, '************  End Stats  ************');
      fnd_file.put_line (1, ' ');
      fnd_file.put_line (1, ' ');

      --
      IF (   (l_error_dc001_count > 0)
          OR (l_error_dc004_count > 0)
          OR (l_error_dc005_count > 0)
         )
      THEN
         generate_dc_error_report;
         retcode := 1;
      ELSE
         retcode := 0;
      END IF;

      generate_dc_end_report;
   EXCEPTION
      WHEN g_end_program
      THEN
         retcode := 2;
         fnd_file.put_line (1, ' ');
         fnd_file.put_line (1,
                            'ERROR!! Processing the Mercer Inbound DC file.'
                           );
         fnd_file.put_line (1, 'ERRMSG: ' || errbuf);
         ROLLBACK;
      WHEN OTHERS
      THEN
         errbuf := SQLERRM;
         retcode := 2;
         ROLLBACK;
   END process_dc_file;
--
--
--
END XXARG_MERCER_INBOUND_PKG;

/