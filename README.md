# BiosignalCDM
2021 바이오협회 데이터톤을 위한 repo 


TODO : 
1. ICU room 바뀌거나, 24시간 지나면 ICU admission id 새로 부여
2. 각 환자별로 첫번째 ICU admission만 걸러냄
3. 먼저 death인 환자들 뽑음. index date는 death time
4. death 없는 환자들은 입원 시작 후 6시간 제외하고 나머지 시간 중에서 랜덤으로 선택됨
5. cohort로 만들어버림
6. Death 이전 24시간 전 EMR and 6시간~2시간 전 생체신호 추출
7. 6시간~2시간 전 생체신호 무결성 체크(admission id 별로)
8. 무결한 환자들은 test, valid dataset으로 보냄. 나머지는 training set으로
9. Death/non-death 숫자보고 최소 5% 비율 맞춰서 undersampling. Non-death 무작위로 삭제함
10. Test 중 30%는 validation set으로 보냄

11. preds의 경우...s3에 업로드하면 하루 2번 자동으로 점수 계산해서 올려드림. 
