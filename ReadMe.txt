Students:
Netanel Levi - 208463158
Dor Yaron - 316083542


Link to Output files - https://emr-logs-mevuzarot.s3.amazonaws.com/output/

How to run the project:
upload the .jar files to s3 :
step1.jar
step2.jar
step3.jar
step4.jar
step5.jar
step6.jar
create an output folder for the output and for the logs
Run Main.java class



Statistics:
(for step 1)
without local aggregation we the number of Reduce input recodes is - 23991894
and their size -
Physical memory (bytes) snapshot=35046412288
Virtual memory (bytes) snapshot=205854011392

with local aggregation we the number of Reduced input recodes is - 600401
and their size - 
Physical memory (bytes) snapshot=33294630912
Virtual memory (bytes) snapshot=205822668800


(for step 3)
without local aggregation we the number of Reduce input recodes is - 1500941
and their size -
Physical memory (bytes) snapshot=28452581376
Virtual memory (bytes) snapshot=215235567616

with local aggregation we the number of Reduced input recodes is - 350559
and their size - 
Physical memory (bytes) snapshot=28799668224
Virtual memory (bytes) snapshot=215782092800



Analysis:
we choosed 10 Trigrams and their top 5 next word:


חיי החברה והתרבות	1.634286363833534E-6
חיי החברה היהודית	1.1032945360620388E-6
חיי החברה והכלכלה	6.375010474816618E-7
חיי החברה והמדינה	5.961591545980875E-7
חיי החברה האנושית	5.912962549305267E-7

ידי הממשלה הבריטית	2.3395727220486455E-6
ידי הממשלה הזמנית	1.2300784161583314E-6
ידי הממשלה הרוסית	8.826287690156652E-7
ידי הממשלה התורכית	8.452852235132743E-7
ידי הממשלה האנגלית	7.25449013994403E-7

נשמע קול נפץ	     2.6793585392988766E-6
נשמע קול צעדים	1.7633456555334552E-6
נשמע קול רעש  	1.549581510274831E-6
נשמע קול קורא	     1.3576426432005833E-6
נשמע קול בכי	     1.0262647744762158E-6

תמורת תשלום גבוה	1.1544779127794643E-6
תמורת תשלום שנתי	9.409064718760932E-7
תמורת תשלום הגון	9.212416633095154E-7
תמורת תשלום סמלי	7.247145102008857E-7
תמורת תשלום מס	6.633081166934206E-7

באותם הימים היתה	3.657507923851138E-6
באותם הימים הקשים	8.647343605922106E-7
באותם הימים הראשונים	8.246090096485795E-7
באותם הימים בא	7.698018873182898E-7
באותם הימים הגיע	7.482399402652372E-7

בתולדות היהודים בארץ	2.4215598540108243E-6
בתולדות היהודים בימי	1.3340021031376686E-6
בתולדות היהודים בארצות	6.633081166934206E-7
בתולדות היהודים בפולין	4.862858619720024E-7
בתולדות היהודים ברוסיה	4.862858619720024E-7

עלה בידם להשיג	9.576949295975789E-7
עלה בידם למצוא	7.247145102008857E-7
עלה בידם להגיע	6.562161609384337E-7
עלה בידם להוציא	4.848064782011987E-7
עלה בידם לכבוש	4.158936418796513E-7

שאין דעתו מיושבת	2.994632550772401E-6
שאין דעתו נוחה	2.4215598540108243E-6
שאין דעתו פנויה	1.2806193611606797E-6
שאין דעתו לחזור	1.255680592937955E-6
שאין דעתו צלולה	3.4687686725236104E-7


תקופת העלייה השנייה	2.3087422896263957E-6
תקופת העלייה הראשונה	1.8726699468217955E-6
תקופת העלייה השלישית	1.168787820874623E-6
תקופת העלייה ההמונית	5.651289337998088E-7
תקופת העלייה הרביעית	5.405476636898112E-7

אמצע המאה התשע	2.684116405807018E-6
אמצע המאה העשרים	2.0735427376920607E-6
אמצע המאה העשירית	9.964737646666645E-7
אמצע המאה השנייה	9.200730882894814E-7
אמצע המאה השמונה	8.464583522296393E-7

We can see from the trigrams above that Given the 2 trigrams "x1 x2 x3" and "y1 y2 y3". Even if the third word "x3" is smaller in alphabetical order than "y3", if the probability for trigram  "x1 x2 x3" is greater than the probability to "y1 y2 y3", than "x1 x2 x3" will be higher on the list.



Step1:
parse the input lines from the corpus and check that the Trigram is valid.
divide the corpus into two parts - part0 and part1 based on their key.
	
Step2:
calculates the "r" base on the occurrences in part0+part1

Step3:
Calculates and writes the N's and T's 

Step4:
Takes the corresponding N's and T's from the Trigrams and combine them into a single record

Step5:
Calculates the probability according to the formula

Step6:
Sort the Trigrams according to the first two words and then according to the probability

