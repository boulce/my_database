# database_system_assignment
2024 Spring Database System Assignment Repository

-part1 설계 변경사항
* 생성할 테이블의 외래키는 고려하지 않는다.
* char배열과, int link를 byte 배열로 변환하여 한 번에 블록단위로 .txt파일이 아니라 .tbl 바이너리 파일에 출력한다.
* 테스트 시나리오에서 primary key 오름차순으로 정렬하는데, 숫자 크기 오름차순이 아니라 문자열 사전순 오름차순이므로 id10이 id9 다음이 아니라 id1 다음에 오게된다.

-고민한점
* 최대한 byte에 직접 접근하지 않고 Block이나 Record 클래스로 변경하도록 노력했다. 즉 읽은 byteBlock -> Block -> Record -> byte 이런 식의 변환이 가능하도록 하여 byte에 직접 접근하지 않고 객체의 값을 수정하도록 하였다. QueryEvaluationEngine 부분에서 직접 접근하는 부분이 있긴한데 나중에 이런 식으로 다듬으면 좋을것 같다
* 리팩토링에 신경썼다. 코드의 의미 단위로 하나의 메소드로 묶었고, 한 클래스에 모든 메소드를 때려박지 않고 설계한대로 메소드들을 나누어 담았다. 그리고 출력부분이랑 Processing 부분이랑 나누도록 노력했다.
* 결과셋을 성의없이 출력하지 않고 테이블 형태로 출력하도록 특수문자 찍는데 시간이 많이 들었다.
