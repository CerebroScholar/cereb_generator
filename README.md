주요 업데이트 : 2018.10.15
  - AKA, 괄호 등에 대한 추가 정제 수행.
  - 실행방법 및 전체 코드구조 기존과 동일. 
  - 현재 크롤링 디비 대상으로 코드 테스트 완료(1015_runningTest.ipnb 참고)
---
일섭 TODO 현재 코드 반영해서 cerebro업데이트. Due : 수요일 점심
---

python version : 3.6.5
required packages : numpy, pandas, nltk, re

use :
<pre>
>>> import cerebDB
>>> generator = cerebDB.CerebDB_Generator()

# From AWS DB
>>> generator = cerebDB.CerebDB_Generator('AWS')
<code>
