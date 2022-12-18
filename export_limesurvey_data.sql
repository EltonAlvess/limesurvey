-- author: elton alves
-- email: eltim.alves@gmail.com
-- date: 14-12-2022
--------------------------------------------------------------
--
CREATE OR REPLACE LANGUAGE plpgsql;
--
CREATE OR REPLACE FUNCTION export_lime_data()
    RETURNS TRIGGER language plpgsql
AS
$export_lime_data$
    DECLARE v_sid		        INT;
            v_gid		        INT;
            v_qid		        INT;
            v_type		        VARCHAR(1);
            v_title		        VARCHAR(10);
            v_question		    VARCHAR(500);
            v_question_order	INT;
            v_table_name	    VARCHAR(50);
            v_parent_qid	    INT;
            v_column_name       VARCHAR(255);
            v_query             TEXT;
            v_response_id       INT;
            v_response_value    VARCHAR(500);
            v_submitdate        TIMESTAMP;
            v_customer_cpf      VARCHAR(20);
            v_response          varchar(500);
            v_created_at        timestamptz;
            v_sub_question      varchar(200);

    -- ## Cursors
    DECLARE curQuestions    refcursor;
            curResponses    refcursor;
            curSubQuestions refcursor;
            curCustomerCpf  refcursor;
            curFinal        refcursor;
    BEGIN
		v_table_name := TG_TABLE_NAME::regclass::text;
        --
		DROP TABLE IF EXISTS temp_result_data;
        DROP TABLE IF EXISTS temp_customer_cpf;
		--
		CREATE TEMPORARY TABLE temp_result_data(
			id				    INT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY NOT NULL,
			response_id	        INT             NOT NULL,
			survey_id			INT             NOT NULL,
			question		    VARCHAR(500)    NULL,
			response            VARCHAR(500)    NULL,
		    question_order      INT             NOT NULL,
		    parent_qid          INT             NOT NULL,
		    question_type       VARCHAR(1)      NULL,
		    created_at          TIMESTAMPTZ DEFAULT NOW(),
		    title               VARCHAR(100)    NOT NULL,
		    submit_date         TIMESTAMP       NULL,
		    cpf                 VARCHAR(20)     NULL);

        CREATE TEMPORARY TABLE temp_customer_cpf(
            response_id INT NOT NULL,
            cpf         VARCHAR(20) NOT NULL);

		OPEN curQuestions FOR
			SELECT	lq.sid,
					lq.gid,
					lq.qid,
					lq.type,
					lq.title,
					lql.question,
					lq.question_order,
					lq.parent_qid
			FROM lime_groups AS lg
			INNER JOIN lime_group_l10ns AS lgl ON lgl.gid = lg.gid
			INNER JOIN lime_questions AS lq ON lq.gid = lgl.gid
			INNER JOIN lime_question_l10ns AS lql ON lql.qid = lq.qid
			WHERE lq.gid = lgl.gid
			AND lq.parent_qid = 0
			AND lq.sid = cast(substring(v_table_name from '(\d{6})$') as integer)
			ORDER BY lq.question_order;
		FETCH curQuestions INTO
			v_sid,
			v_gid,
			v_qid,
			v_type,
			v_title,
			v_question,
			v_question_order,
		    v_parent_qid;
		WHILE FOUND
		LOOP
                IF(v_title = 'CPF' AND v_response_value IS NOT NULL) THEN
                    INSERT INTO temp_customer_cpf(response_id, cpf) VALUES(NEW.id, v_response_value);
                END IF;

                INSERT INTO temp_result_data(response_id, survey_id, question, response, question_order, parent_qid, question_type, title, submit_date, cpf) VALUES(NEW.id,v_sid,v_question, v_response_value, v_question_order, v_parent_qid, v_type, v_title, v_submitdate, null);
		    FETCH curQuestions INTO v_sid, v_gid, v_qid, v_type, v_title, v_question, v_question_order;
		END LOOP;
		CLOSE curQuestions;
        -- subQuestions
		OPEN curSubQuestions FOR
			SELECT	lq.sid,
					lq.gid,
					lq.qid,
					lq.type,
					lq.title,
					lql.question,
					lq.question_order,
					lq.parent_qid
			FROM lime_groups AS lg
			INNER JOIN lime_group_l10ns AS lgl ON lgl.gid = lg.gid
			INNER JOIN lime_questions AS lq ON lq.gid = lgl.gid
			INNER JOIN lime_question_l10ns AS lql ON lql.qid = lq.qid
			WHERE lq.gid = lgl.gid
			AND lq.parent_qid > 0
			AND lq.sid = cast(substring(v_table_name from '(\d{6})$') as integer)
			ORDER BY lq.question_order;
		FETCH curSubQuestions INTO
			v_sid,
			v_gid,
			v_qid,
			v_type,
			v_title,
			v_question,
			v_question_order,
		    v_parent_qid;
		WHILE FOUND
		LOOP
            v_column_name := CAST(v_sid AS VARCHAR(100)) || 'X' || CAST(v_gid AS VARCHAR(100)) || 'X' || CAST(v_parent_qid AS VARCHAR(100)) || CAST(v_title AS VARCHAR(5));
            --
            DROP TABLE IF EXISTS temp_responses;
            v_query := 'CREATE TABLE temp_responses AS SELECT id, "' || v_column_name || '", submitdate FROM ' || v_table_name || ' WHERE submitdate IS NOT NULL AND ' || ' id = ' || NEW.id;
            EXECUTE (v_query);
            --
            OPEN curResponses FOR SELECT * FROM temp_responses;
            FETCH curResponses INTO
                v_response_id,
                v_response_value,
                v_submitdate;
            WHILE FOUND
            LOOP
                INSERT INTO temp_result_data(response_id, survey_id, question, response, question_order, parent_qid, title, submit_date) VALUES(NEW.id,v_sid,v_question, v_response_value, v_question_order, v_parent_qid, v_title, v_submitdate);
                FETCH curResponses INTO v_response_id, v_response_value;
            END LOOP;
            CLOSE curResponses;
            --
		    FETCH curSubQuestions INTO v_sid, v_gid, v_qid, v_type, v_title, v_question, v_question_order, v_parent_qid;
		END LOOP;
		CLOSE curSubQuestions;
        --
		DROP TABLE IF EXISTS temp_responses;
		--
		OPEN curCustomerCpf FOR SELECT response_id, cpf FROM temp_customer_cpf;
		    FETCH curCustomerCpf INTO
		        v_response_id,
		        v_customer_cpf;
		    WHILE FOUND
		    LOOP
                UPDATE temp_result_data
                SET cpf = v_customer_cpf
                WHERE response_id = NEW.id;

                FETCH curCustomerCpf INTO v_response_id, v_customer_cpf;
            END LOOP;
		CLOSE curCustomerCpf;
		--
        --RETURN QUERY
		OPEN curFinal FOR
            SELECT trd.response_id,
                   trd.survey_id,
                   lql.question,
                   trd.question,
                   CASE
                        WHEN trd.response = '' THEN NULL
                   ELSE
                       trd.response
                   END AS response,
                   trd.parent_qid,
                   trd.question_order,
                   trd.created_at,
                   trd.title,
                   trd.submit_date,
                   trd.cpf
            FROM temp_result_data trd
            INNER JOIN lime_question_l10ns lql ON lql.qid = trd.parent_qid
        UNION ALL
            SELECT response_id,
                   survey_id,
                   question,
                   null,
                   response,
                   parent_qid,
                   question_order,
                   created_at,
                   title,
                   submit_date,
                   cpf
            FROM temp_result_data
            WHERE parent_qid = 0
            ORDER BY parent_qid, question_order;
		FETCH curFinal INTO
		    v_response_id,
		    v_sid,
		    v_question,
		    v_response,
		    v_sub_question,
		    v_parent_qid,
		    v_question_order,
		    v_created_at,
		    v_title,
		    v_submitdate,
		    v_customer_cpf;
		WHILE FOUND
		LOOP
		    IF(NEW.submitdate IS NOT NULL) THEN
                INSERT INTO lime_exported_data(id_response, id_survey, question_desc, sub_question, response_value, qid_parent, q_order, inserted_at, title_response, submitdate, customer_cpf)
                    VALUES(NEW.id,v_sid,v_question,v_response,v_sub_question,v_parent_qid,v_question_order,v_created_at,v_title,v_submitdate,v_customer_cpf);
            END IF;
            FETCH curFinal INTO v_response_id,v_sid,v_question,v_response,v_sub_question,v_parent_qid,v_question_order,v_created_at,v_title,v_submitdate,v_customer_cpf;
        END LOOP;
		CLOSE curFinal;
		--
        RETURN NULL;
    END;
$export_lime_data$;

alter function 
export_lime_data() owner to postgres;
