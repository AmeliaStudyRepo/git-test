# Databricks notebook source
#Dataframe 만들기

import pandas as pd

data = [[1, "KYO"], [2, "YSH"], [3, "JMS"], [4, "KCS"], [5, "KYH"]]

pdf = pd.DataFrame(data, columns=["id", "name"])

df1 = spark.createDataFrame(pdf)
df2 = spark.createDataFrame(data, schema="id LONG, name STRING")

# COMMAND ----------

#Dataframe 확인
pdf

# COMMAND ----------

#Dataframe 열추가
#<DF명>['추가할 열 이름'] = [추가할 값]

pdf['Gender'] = ['F', 'M', 'M', 'M', 'F']
pdf

# COMMAND ----------

#Dataframe 행추가
#<DF명>.loc[추가될 인덱스 번호] = [추가할 값]

pdf.loc[5] = ['6', 'KDP', 'M']
pdf

# COMMAND ----------

#Dataframe 값 변환
#<DF명>.loc[행의 인덱스, 열 이름] = 바꿀 값

pdf.loc[5, 'name'] = ['KYC']
pdf

# COMMAND ----------

#특정열로 정렬하기

pdf['name'].sort_values()

# COMMAND ----------

#특정열로 정렬하기(내림차순)
pdf['name'].sort_values(ascending = False)

# COMMAND ----------

#인덱스 값 기준으로 데이터 정렬
pdf['name'].sort_index()

# COMMAND ----------

#Dataframe concat - 두 개의 데이터 프레임을 합칠 때 사용

#1 concat 실습용으로 dataframe 2개 생성

import pandas as pd
df1 = pd.DataFrame({'name' : ['bok', 'jeong', 'yoon', 'kim', 'jin', 'lee'], 'code' : [1, 2, 3, 4, 5, 6]})
df2 = pd.DataFrame({'name' : ['choi', 'wang', 'hong', 'park', 'han', 'moon'], 'code' : [7, 8, 9, 10, 11, 12]})


# COMMAND ----------

df1

# COMMAND ----------

df2

# COMMAND ----------

#2 행 기준 합치기
#행 기준 합치기의 경우 일치하지 않은 column이 존재할 때는 NaN 처리

pd.concat([df1, df2], axis = 0)

# COMMAND ----------

#3 열 기준 합치기

pd.concat([df1, df2], axis = 1)

# COMMAND ----------

#Dataframe merge - 공통열을 기준으로 두 데이터프레임 병합(SQL에서 join과 동일)
#left : 왼쪽 데이터프레임
#right : 오른쪽 데이터프레임
#on : (두 데이터프레임의 기준열 이름이 같을 때) 기준열
#left_on : 기준열 이름이 다를 때, 왼쪽 기준열
#right_on : 기준열 이름이 다를 때, 오른쪽 기준열
#how : 조인 방식 {'left', 'right', 'inner', 'outer'}  / 기본값은 'inner'

# 1. merge를 위한 dataframe 생성

mdf1 = pd.DataFrame({'code' : [1, 2, 3, 4, 5, 6, 7], 'student' : ['sammy', 'harry', 'radi', 'hailey', 'warren', 'teo', 'sena']})
mdf2 = pd.DataFrame({'code' : [1, 4, 5, 6, 7, 13, 14], 'grade' : [5, 8, 10, 3, 5, 7, 2]})


# 기준열 이름이 같을 때
#pd.merge(left, right, on = '기준열', how = '조인방식')

pd.merge(mdf1, mdf2, on = 'code', how = 'left')

#빈 값은 NaN 처리


# COMMAND ----------

# 기준열 이름이 다를 때
#pd.merge(left, right, left_on = '왼쪽 열', right_on = '오른쪽 열', how = '조인방식')

pd.merge(mdf1, mdf2, left_on = 'code', right_on = 'grade', how = 'right')

# COMMAND ----------

#이 외에도 join 방식에 따른 종류가 많음. 직접 찾아보기 권장.

#concat - 2개 이상의 데이터프레임을 한번에 병합 가능 / 합집합 형태
#merge - 데이터프레임이 2개인 경우만 join 가능 / '특정 공통열'을 기준으로, 나머지 열까지 join하고 싶을 때 권장

# COMMAND ----------

#Dataframe scenario

# COMMAND ----------


#1 실습용으로 dataframe 2개 생성

# 환자 데이터 생성
patient_data = {
    "Code": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111],
    "Name": ["Andrew", "Clara", "Daniel", "Emma", "Henry", "Oliver", "Paul", "Peter", "Sarah", "Tiffany", "William"],
    "Gender": ["m", "f", "m", "f", "m", "m", "m", "m", "f", "f", "m"],
    "Phone_Number_lastdigit": [5924, 1973, 3481, 2752, 9873, 6237, 3519, 8348, 2525, 5749, 6905]
}

# 질병 데이터 생성
symptom_data = {
    "ID":[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    "Title":["flu", "food poisonig", "diarrhea", "stomach pain", "headache", "hay fever", "asthma", "rhinitis",  "anemia", "burn", "lcoholism", "depression", "acne", "arthritis" , "diabetes"],
    "Severity_Level":["5", "7", "4", "6", "7", "3", "3", "6", "1", "9", "8", "9", "2", "3", "6"],
}

# 각각의 데이터 프레임 생성
patient_df = pd.DataFrame(patient_data)
symptom_df = pd.DataFrame(symptom_data)

# COMMAND ----------

patient_df

# COMMAND ----------

symptom_df

# COMMAND ----------

# 2023년 1월 병원 방문 기록 데이터
log_jan = {
    "Code":[101, 108, 101, 102, 110, 105, 110, 110, 108, 109],
    "ID":[6, 3, 7, 4, 2, 1, 6, 8, 9, 6],
    "Date":['2023-01-02', '2023-01-03', '2023-01-04', '2023-01-11', '2023-01-12', '2023-01-23', '2023-01-25', '2023-01-27', '2023-01-28', '2023-01-29']
}

# 2023년 2월 병원 방문 기록 데이터
log_feb = {
    "Code":[105, 102, 102, 106, 108, 106, 102, 106, 107, 110],
    "ID":[7, 9, 2, 4, 6, 8, 3, 2, 3, 1],
    "Date":['2023-02-08', '2023-02-08', '2023-02-08', '2023-02-11', '2023-02-17', '2023-02-18', '2023-02-19', '2023-02-19', '2023-02-23', '2023-02-28']
}

# 각각의 데이터 프레임 생성
log_jan = pd.DataFrame(log_jan)
log_feb = pd.DataFrame(log_feb)

# COMMAND ----------

log_jan

# COMMAND ----------

log_feb

# COMMAND ----------

log_jan.dtypes

# COMMAND ----------

log_feb.dtypes

# COMMAND ----------

log_jan['Date'] = pd.to_datetime(log_jan['Date'], infer_datetime_format=True)
log_feb['Date'] = pd.to_datetime(log_jan['Date'], infer_datetime_format=True)

# COMMAND ----------

log_jan.dtypes

# COMMAND ----------

log_feb.dtypes

# COMMAND ----------

#생성된 dataframe
patient_df
symptom_df
log_jan
log_feb

# COMMAND ----------

patient_df

# COMMAND ----------

symptom_df

# COMMAND ----------

import random
main = [patient_df, symptom_df]
random.shuffle(main)
mainchart = pd.concat(main, axis=1)

# COMMAND ----------

mainchart.dtypes

# COMMAND ----------

mainchart['Code'] = mainchart['Code'].fillna(-1).astype('int64').replace({-1: None})
mainchart['Phone_Number_lastdigit'] = mainchart['Phone_Number_lastdigit'].fillna(-1).astype('int64').replace({-1: None})

print(mainchart.dtypes)

# COMMAND ----------

mainchart

# COMMAND ----------

mainchart.drop(index = [11, 12, 13,14])

# COMMAND ----------

final_log = pd.concat([log_jan, log_feb], axis = 0)
final_log['Code'].sort_values()
final_log

# COMMAND ----------

import pandas as pd
final_chart0 = pd.merge(mainchart, final_log, on = 'Code', how = 'left')
final_chart0

# COMMAND ----------

final_chart = final_chart0.drop(index = [23, 24, 25, 26])
final_chart

# COMMAND ----------

print(final_chart.dtypes)

# COMMAND ----------

#최근 방문일자 순 정렬
final_chart.sort_values(by='Date')

# COMMAND ----------

#중증도 순으로 정렬
final_chart.sort_values(by='Severity_Level')

# COMMAND ----------

#성별이 여성인 환자차트 만들기
female_chart = final_chart['Gender'] == 'f'
female_chart = final_chart[female_chart]
female_chart

# COMMAND ----------

final_chart = pd.DataFrame(final_chart)

# COMMAND ----------

chart = spark.createDataFrame(final_chart)
chart.write.mode("overwrite").saveAsTable("tb_finalchart")

# COMMAND ----------


