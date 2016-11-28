CREATE TABLE rechnung (
  idrechnung integer NOT NULL AUTO_INCREMENT,
  datum date ,
  konto1 varchar(45) ,
  konto2 varchar(45) ,
  betrag decimal(10,2) ,
  buchungstext varchar(1000),
  PRIMARY KEY (idrechnung)
) ;
