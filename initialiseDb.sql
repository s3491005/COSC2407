CREATE TABLE "STATE" (
    "ID" CHAR(1) NOT NULL,
    "SHORT_NAME" VARCHAR(3) NOT NULL,
    PRIMARY KEY ("ID")
);

INSERT INTO "STATE" VALUES ('A', 'ACT');
INSERT INTO "STATE" VALUES ('N', 'NSW');
INSERT INTO "STATE" VALUES ('O', 'NT');
INSERT INTO "STATE" VALUES ('Q', 'QLD');
INSERT INTO "STATE" VALUES ('S', 'SA');
INSERT INTO "STATE" VALUES ('T', 'TAS');
INSERT INTO "STATE" VALUES ('V', 'VIC');
INSERT INTO "STATE" VALUES ('W', 'WA');

CREATE TABLE "STATUS" (
    "ID" CHAR(1) NOT NULL,
    "NAME" VARCHAR(12) NOT NULL,
    PRIMARY KEY ("ID")
);

INSERT INTO "STATUS" VALUES ('D', 'Deregistered');
INSERT INTO "STATUS" VALUES ('R', 'Registered');

CREATE TABLE "BUSINESS_NAME" (
    "ID" INTEGER NOT NULL,
    "NAME" VARCHAR(200),
    "STATUS" CHAR(1),
    "REG_DT" DATE,
    "CANCEL_DT" DATE,
    "RENEW_DT" DATE,
    "STATE_NUM" VARCHAR(10),
    "STATE_OF_REG" CHAR(1),
    "ABN" VARCHAR(13),
    PRIMARY KEY ("ID"),
    FOREIGN KEY ("STATUS") REFERENCES "STATUS"("ID"),
    FOREIGN KEY ("STATE_OF_REG") REFERENCES "STATE"("ID")
);
