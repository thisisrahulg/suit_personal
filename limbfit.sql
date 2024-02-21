CREATE TABLE IF NOT EXISTS limbfitDatabase.limbfit (
	`INDEX` INT auto_increment NOT NULL PRIMARY KEY COMMENT 'Counter',
	`TIME` DATETIME NOT NULL COMMENT 'Date-time of the FITS file.',
	XC FLOAT NOT NULL COMMENT 'X-val of the centre',
	XERROR FLOAT NOT NULL COMMENT 'X-error',
	YC FLOAT NOT NULL COMMENT 'Y-value of the centre',
	YERROR FLOAT NOT NULL COMMENT 'Y-error',
	RC FLOAT NOT NULL COMMENT 'Radius from the fit.',
	RERROR FLOAT NOT NULL COMMENT 'Error in radius fit.',
	`MOD_TIME` FLOAT NOT NULL COMMENT 'Modification time of the file.'
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_general_ci
COMMENT='Limbfit database for SUIT';
