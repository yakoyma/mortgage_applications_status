"""
===============================================================================
Data Analytics Project: Determine the Status of Mortgage Applications with SQL
===============================================================================
1. Data Analysis
2. SQL Queries
3. Save the Mortgage Applications Status Table
"""
# Standard libraries
import platform
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Other libraries
import polars as pl
import ydata_profiling


from polars import read_excel, SQLContext
from ydata_profiling import ProfileReport


# Display versions of platforms and packages
print('\nPython: {}'.format(platform.python_version()))
print('Polars: {}'.format(pl.__version__))
print('YData-profiling: {}'.format(ydata_profiling.__version__))



"""
===============================================================================
1. Data Analysis
===============================================================================
"""
# Load datasets
INPUT_XLSX_1 = 'datasets/crédit breton_demandes de prêt.xlsx'
INPUT_XLSX_2 = 'datasets/crédit breton_agences.xlsx'
INPUT_XLSX_3 = 'datasets/crédit breton_situation pro.xlsx'
INPUT_XLSX_4 = 'datasets/crédit breton_apport.xlsx'
INPUT_XLSX_5 = 'datasets/crédit breton_situation familiale.xlsx'

mortgage_applications = read_excel(source=INPUT_XLSX_1)
mortgage_applications_report = ProfileReport(
    df=mortgage_applications.to_pandas(),
    title='Mortgage Applications Dataset Report'
)
mortgage_applications_report.to_file(
    'reports/mortgage_applications_report.html')
print(f'\n\n\nMortgage applications:\n{mortgage_applications}')

branches = read_excel(source=INPUT_XLSX_2)
branches_report = ProfileReport(
    df=branches.to_pandas(),
    title='Branches Dataset Report'
)
branches_report.to_file('reports/branches_report.html')
print(f'\n\nBranches:\n{branches}')

pro_status = read_excel(source=INPUT_XLSX_3)
pro_status_report = ProfileReport(
    df=pro_status.to_pandas(),
    title='Professional Status Dataset Report'
)
pro_status_report.to_file('reports/pro_status_report.html')
print(f'\n\nProfessionnal status:\n{pro_status}')

down_payment = read_excel(source=INPUT_XLSX_4)
down_payment_report = ProfileReport(
    df=down_payment.to_pandas(),
    title='Down Payment Dataset Report'
)
down_payment_report.to_file('reports/down_payment_report.html')
print(f'\n\nDown payment:\n{down_payment}')

family_status = read_excel(source=INPUT_XLSX_5)
family_status_report = ProfileReport(
    df=family_status.to_pandas(),
    title='Family Status Dataset Report'
)
family_status_report.to_file('reports/family_status_report.html')
print(f'\n\nFamily status:\n{family_status}')



"""
===============================================================================
2. SQL Queries
===============================================================================
"""
# Record all DataFrames for queries
with SQLContext(register_globals=True, eager=True) as ctx:

    """
    Query 1:
    Selection of all granted applications (the 'O' values in the Accord column) 
    from the mortgage_applications table and sort the resulting table in 
    descending order using the Montant_opération column.
    """
    query_1 = """
    SELECT * FROM mortgage_applications
    WHERE Accord = 'O'
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\n\nResult of the query 1:\n{ctx.execute(query_1)}')


    """
    Query 2:
    Selection of all refused applications (the 'N' values in the Accord column) 
    from the mortgage_applications table and sort the resulting table in 
    descending order using the Montant_opération column.
    """
    query_2 = """
    SELECT * FROM mortgage_applications
    WHERE Accord = 'N'
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\nResult of the query 2:\n{ctx.execute(query_2)}')


    """
    Query 3:
    Selection of all processed applications granted or refused (respectively 
    the 'O' or 'N' values in the Accord column) from the mortgage_applications 
    table and sort the resulting table in descending order using the 
    Montant_opération column.
    """
    query_3 = """
    SELECT * FROM mortgage_applications
    WHERE Accord IS NOT NULL
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\nResult of the query 3:\n{ctx.execute(query_3)}')


    """
    Query 4:
    Selection of all unprocessed applications (the null values in the Accord 
    column) from the mortgage_applications table and sort the resulting table 
    in descending order using the Montant_opération column.
    """
    query_4 = """
    SELECT * FROM mortgage_applications
    WHERE Accord IS NULL
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\nResult of the query 4:\n{ctx.execute(query_4)}')


    """
    Query 5:
    - Firstly, select all applications from the mortgage_applications table.
    - Then, select the values in the Montant_opération column that exceed 
      100000 €.
    - Finally, sort the resulting table in descending order using the 
      Montant_opération column.
    """
    query_5 = """
    SELECT * FROM mortgage_applications
    WHERE Montant_opération > 100000
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\nResult of the query 5:\n{ctx.execute(query_5)}')


    """
    Query 6:
    - Firstly, select the Numéro_client, Nombre_demandes_de_prêts (the count 
      of the values in the Numéro_demande_de_prêt column in order to get the 
      total number of all applications from a single applicant), and 
      Montant_total_opérations (the sum of the values in the Montant_opération
      column in order to get the total amount of all applications from a 
      single applicant) columns from the mortgage_applications table, 
    - Then, the resulting table is grouped by the Numéro_client column and 
      select the values in the Montant_total_opérations column greater than or 
      equal to 300000 €. 
    - Finally, sort the resulting table in descending order using the
      Nombre_demandes_de_prêts column.
    """
    query_6 = """
    SELECT
        Numéro_client,
        COUNT(Numéro_demande_de_prêt) AS Nombre_demandes_de_prêts,
        SUM(Montant_opération) AS Montant_total_opérations,
    FROM mortgage_applications
    GROUP BY Numéro_client
    HAVING Montant_total_opérations >= 300000
    ORDER BY Nombre_demandes_de_prêts DESC;
    """
    print(f'\n\nResult of the query 6:\n{ctx.execute(query_6)}')


    """    
    Query 7:
    - Firstly, select the Numéro_client, Numéro_demande_de_prêt, 
      Montant_opération, Date_de_demande, and Année (resulting from the 
      transformation of the year part of the Date_de_demande column) columns 
      from the mortgage_applications table.
    - Then, select the values in the Année column between 2018 and 2019. 
    - Finally, sort the resulting table in ascending order using the Année 
      column.
    """
    query_7 = """
    SELECT
        Numéro_client,
        Numéro_demande_de_prêt,
        Montant_opération,
        Date_de_demande,
        DATE_PART('year', Date_de_demande) AS Année
    FROM mortgage_applications
    WHERE DATE_PART('year', Date_de_demande) IN (2018, 2019)
    ORDER BY Année ASC;
    """
    print(f'\n\nResult of the query 7:\n{ctx.execute(query_7)}')


    """
    Query 8:
    - Firstly, select the Numéro_client, Numéro_demande_de_prêt, 
      Montant_opération, Date_de_demande, and Année (resulting from the 
      transformation of the year part of the Date_de_demande column) columns 
      from the mortgage_applications table.
    - Then, select the values in the Année column between 2020 and 2021.
    - Finally, sort the resulting table in descending order using the Année 
      column.
    """
    query_8 = """
    SELECT
        Numéro_client,
        Numéro_demande_de_prêt,
        Montant_opération,
        Date_de_demande,
        DATE_PART('year', Date_de_demande) AS Année
    FROM mortgage_applications
    WHERE DATE_PART('year', Date_de_demande) IN (2020, 2021)
    ORDER BY Année DESC;
    """
    print(f'\n\nResult of the query 8:\n{ctx.execute(query_8)}')


    """
    Query 9:
    Selection of Numéro_client, Date_de_demande, Montant_opération, Jour 
    (resulting from the transformation of the day part of the Date_de_demande 
    column), Mois (resulting from the transformation of the month part of the 
    Date_de_demande column), and Année (resulting from the transformation of 
    the year part of the Date_de_demande column) columns from the 
    mortgage_applications table.
    """
    query_9 = """
    SELECT
        Numéro_client,
        Date_de_demande,
        Montant_opération,
        DATE_PART('day', Date_de_demande) AS Jour,
        DATE_PART('month', Date_de_demande) AS Mois,
        DATE_PART('year', Date_de_demande) AS Année
    FROM mortgage_applications;
    """
    print(f'\n\nResult of the query 9:\n{ctx.execute(query_9)}')


    """
    Query 10:
    Selection of the Numéro_client, Date_de_demande, Montant_opération, 
    Trimestre (resulting from the transformation of the quarter part of the
    Date_de_demande column), Année (resulting from the transformation of the 
    year part of the Date_de_demande column), and Décennie (resulting from the 
    transformation of the decade part of the Date_de_demande column) columns 
    from the mortgage_applications table.
    """
    query_10 = """
    SELECT
        Numéro_client,
        Date_de_demande,
        Montant_opération,
        EXTRACT(quarter FROM Date_de_demande) AS Trimestre,
        EXTRACT(year FROM Date_de_demande) AS Année,
        EXTRACT(decade FROM Date_de_demande) AS Décennie
    FROM mortgage_applications;
    """
    print(f'\n\nResult of the query 10:\n{ctx.execute(query_10)}')


    """
    Query 11:
    - Firstly, select the Numéro_client and Montant_total_opérations (the 
      sum of the values in the Montant_opération column in order to get the 
      total amount of all applications from a single applicant) columns from 
      the mortgage_applications.
    - Then, the resulting table is grouped by the Numéro_client column and 
      select the values in the Montant_total_opérations column that exceed 
      300000 €.
    - Finally, sort the resulting table in descending order using the 
      Montant_total_opérations column.
    """
    query_11 = """
    SELECT
        Numéro_client,
        SUM(Montant_opération) AS Montant_total_opérations
    FROM mortgage_applications
    GROUP BY Numéro_client
    HAVING Montant_total_opérations > 300000
    ORDER BY Montant_total_opérations DESC;
    """
    print(f'\n\nResult of the query 11:\n{ctx.execute(query_11)}')


    """
    Query 12:
    Selection of the Numéro_demande_de_prêt, Accord, Montant_opération, 
    and Apport columns in the table resulting from the full join between the 
    mortgage_applications and down_payment tables using the 
    Numéro_demande_de_prêt column, and sort the resulting table in descending 
    order using the Montant_opération column.
    """
    query_12 = """
    SELECT
        COALESCE(
            mortgage_applications.Numéro_demande_de_prêt,
            down_payment.Numéro_demande_de_prêt
        ) AS Numéro_demande_de_prêt,
        Accord,
        Montant_opération,
        Apport
    FROM mortgage_applications
    FULL JOIN down_payment USING (Numéro_demande_de_prêt)
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\nResult of the query 12:\n{ctx.execute(query_12)}')


    """
    Query 13:
    - Firstly, select the Numéro_agence, Ville, and Montant_opération columns 
      in the table resulting from the join between the mortgage_applications 
      and branches tables using the Numéro_agence column.
    - Then, sort the resulting table in descending order using the 
      Montant_opération column.
    - Finally, display only the first 10 rows of the resulting table.
    """
    query_13 = """
    SELECT
        COALESCE(
            mortgage_applications.Numéro_agence,
            branches.Numéro_agence
        ) AS Numéro_agence,
        Ville,
        Montant_opération
    FROM mortgage_applications
    LEFT JOIN branches USING (Numéro_agence)
    ORDER BY Montant_opération DESC LIMIT 10;
    """
    print(f'\n\nResult of the query 13:\n{ctx.execute(query_13)}')


    """
    Query 14:
    - Main query:
      - First, select the Numéro_agence, Montant_total_opérations (the sum of 
        the values in the Montant_opération column in order to get the total 
        amount of all applications from a single applicant), and Ville columns 
        from the branches_performance table.
      - Then, the resulting table is grouped by Numéro_agence and Ville 
        columns. Finally, sort the resulting table in descending order using 
        the Montant_total_opérations column.
    
    - Subquery for the branches_performance intermediate table:
      Selection of the Numéro_agence, Montant_opération, and Ville columns in 
      the table resulting from the left join between the mortgage_applications 
      and branches tables using the Numéro_agence column.
    """
    query_14 = """
    WITH branches_performance AS (
        SELECT
            COALESCE(
                mortgage_applications.Numéro_agence,
                branches.Numéro_agence
            ) AS Numéro_agence,
            Montant_opération,
            Ville
        FROM mortgage_applications
        LEFT JOIN branches USING (Numéro_agence)    
    ),
    SELECT
        Numéro_agence,
        SUM(Montant_opération) AS Montant_total_opérations,
        Ville
    FROM branches_performance
    GROUP BY
        Numéro_agence,
        Ville
    ORDER BY
        Montant_total_opérations DESC;
    """
    print(f'\n\nResult of the query 14:\n{ctx.execute(query_14)}')


    """
    Query 15:
    - Firstly, select the Numéro_client, Montant_opération, 
      Catégorie_socioprofessionnelle, Statut_emploi, and Régularité_des_revenus 
      columns in the table resulting from the left join between the 
      mortgage_applications and pro_status tables using the Numéro_client 
      column.
    - Then, select the values in the Régularité_des_revenus column equal to 
      '3 : Très irréguliers'.
    - Finally, sort the resulting table in descending order using the 
      Montant_opération column.
    """
    query_15 = """
    SELECT
        COALESCE(
            mortgage_applications.Numéro_client,
            pro_status.Numéro_client
        ) AS Numéro_client,
        Montant_opération,
        Catégorie_socioprofessionnelle,
        Statut_emploi,
        Régularité_des_revenus
    FROM mortgage_applications
    LEFT JOIN pro_status USING (Numéro_client)
    WHERE Régularité_des_revenus = '3 : Très irréguliers'
    ORDER BY Montant_opération DESC;
    """
    print(f'\n\nResult of the query 15:\n{ctx.execute(query_15)}')


    """
    Query 16:
    - Main query:
      - Firstly, select the Numéro_client, Nombre_demandes_de_prêts (the count 
        of the values in the Numéro_demande_de_prêt column in order to get the 
        total number of all applications from a single applicant), 
        Montant_total_opérations (the sum of the values in the 
        Montant_opération column in order to get the total amount of all 
        applications from a single applicant), Catégorie_socioprofessionnelle, 
        Statut_emploi, Régularité_des_revenus, and Revenu_mensuel_moyen columns 
        from the pro_situations table.
      - Then, the resulting table is grouped by the Numéro_client, 
        Catégorie_socioprofessionnelle, Statut_emploi, Régularité_des_revenus, 
        and Revenu_mensuel_moyen columns, and select the values in the 
        Régularité_des_revenus column equal to '3 : Très irréguliers'.
      - Finally, sort the resulting table in descending order using the 
        Montant_total_opérations column.
    
    - Subquery for the pro_situations intermediate table: 
      Selection of the Numéro_client, Numéro_demande_de_prêt, 
      Montant_opération, Catégorie_socioprofessionnelle, Statut_emploi, 
      Régularité_des_revenus, and Revenu_mensuel_moyen columns in the table 
      resulting from the right join between the mortgage_applications and 
      pro_status tables using the Numéro_client column.
    """
    query_16 = """
    WITH pro_situations AS (
        SELECT
            COALESCE(
                mortgage_applications.Numéro_client,
                pro_status.Numéro_client
            ) AS Numéro_client,
            mortgage_applications.Numéro_demande_de_prêt,
            mortgage_applications.Montant_opération,
            pro_status.Catégorie_socioprofessionnelle,
            pro_status.Statut_emploi,
            pro_status.Régularité_des_revenus,
            pro_status.Revenu_mensuel_moyen
        FROM mortgage_applications
        RIGHT JOIN pro_status USING (Numéro_client) 
    ),
    SELECT
        Numéro_client,
        COUNT(Numéro_demande_de_prêt) AS Nombre_demandes_de_prêts,
        SUM(Montant_opération) AS Montant_total_opérations,
        Catégorie_socioprofessionnelle,
        Statut_emploi,
        Régularité_des_revenus,
        Revenu_mensuel_moyen
    FROM pro_situations
    GROUP BY
        Numéro_client,
        Catégorie_socioprofessionnelle,
        Statut_emploi, 
        Régularité_des_revenus,
        Revenu_mensuel_moyen
    HAVING
        Régularité_des_revenus = '3 : Très irréguliers'
    ORDER BY
        Montant_total_opérations DESC;
    """
    print(f'\n\nResult of the query 16:\n{ctx.execute(query_16)}')


    """
    Query 17:
    - Firstly, select the Numéro_client, Date_de_demande, Durée, 
      Montant_opération, Durée_annuelle (Dividing the values in the Durée 
      column by 12), Date_de_naissance, and Age (the difference between the 
      values in columns Date_de_demande and Date_de_naissance) columns in the 
      table resulting from the left join between the mortgage_applications and 
      family_status tables using the Numéro_client column.
    - Then, select applicants whose age at the end of the loan period exceeds 
      82 years old. 
    - Finally, sort the resulting table in descending order using the 
      Durée_annuelle column.
    """
    query_17 = """
    SELECT
        COALESCE(
            mortgage_applications.Numéro_client,
            family_status.Numéro_client
        ) AS Numéro_client,
        Date_de_demande,
        Durée,
        Montant_opération,
        DIV(Durée, 12) AS Durée_annuelle,
        Date_de_naissance,
        DATE_PART('year', Date_de_demande) - DATE_PART('year', 
            Date_de_naissance) AS Age
    FROM mortgage_applications
    LEFT JOIN family_status USING (Numéro_client)
    WHERE
        DATE_PART('year', Date_de_demande) - DATE_PART('year', 
            Date_de_naissance) + DIV(Durée, 12) >= 82
    ORDER BY
        Durée_annuelle DESC;
    """
    print(f'\n\nResult of the query 17:\n{ctx.execute(query_17)}')


    """
    Query 18:
    - Main query:
      • Selection of columns:
        - First, select the Numéro_client, Date_de_demande, Montant_opération, 
          Apport, Durée, Montant_du_prêt, Remboursement_mensuel, and Accord 
          columns from the financial_situations table.
        - Then, select the Revenu_mensuel_moyen and Régularité_des_revenus 
          from the pro_status table.
        - Finally, select the Date_de_naissance and Nombre_enfants_à_charge 
          columns from the table family_status.    
      • Calculate the status of the mortgage application:
        - Case 1: The first condition checks whether the applicant's age 
          at the end of the loan period exceeds 82 years old. If this is the 
          case, the mortgage application is rejected.
        - Case 2: If the first condition is true, the second condition checks 
          whether the applicant's average monthly income is very irregular. 
          If this is the case, the mortgage application is rejected.
        - Case 3: If the second condition is true, the third condition checks 
          wether the monthly repayment is greather than 33% of the applicant's 
          average monthly income plus the number of dependent children. If 
          this is the case, the mortgage application is rejected. Otherwise, 
          it is accepted.
    
    - Subquery for the financial_situations intermediate table:
      Selection of the Numéro_demande_de_prêt, Numéro_client, Date_de_demande, 
      Montant_opération, Durée, Accord, Apport, Montant_du_prêt (the monthly 
      amount of the loan is the difference between the values in columns 
      Montant_opération and Apport, divided by the Durée column), 
      Durée_annuelle (Dividing the values in the Durée column by 12), and 
      Remboursement_mensuel columns in the table resulting from the full join
      between the mortgage_applications and down_payment tables using the 
      Numéro_demande_de_prêt column.
    """
    query_18 = """
    WITH financial_situations AS (
        SELECT
            COALESCE(
                mortgage_applications.Numéro_demande_de_prêt,
                down_payment.Numéro_demande_de_prêt
            ) AS Numéro_demande_de_prêt,
            mortgage_applications.Numéro_client,
            mortgage_applications.Date_de_demande,
            mortgage_applications.Montant_opération,
            mortgage_applications.Durée,
            mortgage_applications.Accord,
            down_payment.Apport,
            Montant_opération - Apport AS Montant_du_prêt,
            DIV(Durée, 12) AS Durée_annuelle,
            DIV(Montant_opération - Apport, Durée) AS Remboursement_mensuel
        FROM mortgage_applications
        FULL JOIN down_payment USING (Numéro_demande_de_prêt)    
    ),
    SELECT
        COALESCE(
            mortgage_applications.Numéro_client,
            pro_status.Numéro_client,
            family_status.Numéro_client
        ) AS Numéro_client,
        financial_situations.Date_de_demande,
        financial_situations.Montant_opération,
        financial_situations.Apport,
        financial_situations.Durée,
        financial_situations.Montant_du_prêt,
        financial_situations.Remboursement_mensuel,
        financial_situations.Accord,
        pro_status.Revenu_mensuel_moyen,
        pro_status.Régularité_des_revenus,
        family_status.Date_de_naissance,
        family_status.Nombre_enfants_à_charge,
        CASE
            WHEN DATE_PART('year', Date_de_demande) - DATE_PART( 'year', 
                Date_de_naissance) + Durée_annuelle >= 82 THEN 'Refusé'
            WHEN Régularité_des_revenus = '3 : Très irréguliers' THEN 'Refusé'
            WHEN Remboursement_mensuel > 0.33 * Revenu_mensuel_moyen +  
                Nombre_enfants_à_charge THEN 'Refusé'
            ELSE 'Accepté'
        END AS Statut_demande_de_prêt
    FROM financial_situations
    LEFT JOIN pro_status USING (Numéro_client)
    LEFT JOIN family_status USING (Numéro_client)
    ORDER BY Statut_demande_de_prêt DESC;
    """
    mortgage_applications_status = ctx.execute(query_18)
    print(f'\n\nResult of the query 18:\n{mortgage_applications_status}')



"""
===============================================================================
3. Save the Mortgage Applications Status Table
===============================================================================
"""
mortgage_applications_status.write_excel(
    'datasets/mortgage_applications_status.xlsx')
mortgage_applications_status_report = ProfileReport(
    df=mortgage_applications_status.to_pandas(),
    title='Mortgage Applications Status Dataset Report'
)
mortgage_applications_status_report.to_file(
    'reports/mortgage_applications_status_report.html')
