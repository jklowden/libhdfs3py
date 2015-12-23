BEGIN { in_comment = 0 }

/^[/][*][*]$/ { in_comment = 1; }

/hdfs.+ - / {
    split($0, DESC, "- "); 
    gsub("[.]", "", DESC[2]); 
    gsub("hdfs","",$2); 
    AZ = substr($2, 1, 1); 
    az = tolower(AZ); 
    sub(AZ, az, $2); 
    print "static const char", $2"_doc[]", "= \""DESC[2]"\";"; 
    in_comment = 0;
    next;
}

/^ [*] / {
    if( 1 == in_comment ) {
	sub("[^A-Z]+", "", $0)
	TEXT = $0;
	in_comment = 2;
	next;
    }
}

/^ [*][/]/ {
    if( 2 == in_comment ) {
	in_comment = 3;
    }
    next;
}

/ hdfs[^(]+[(]/ {
    if( 3 == in_comment ) {
	N = split($0, names, "[^A-Za-z0-9_]", SEPS);
	#printf("\t// %d parts in %s\n", N, $0);
	NAME = names[2];
	gsub("hdfs", "", NAME); 
	AZ = substr(NAME, 1, 1); 
	az = tolower(AZ); 
	sub(AZ, az, NAME); 
	print "static const char", NAME"_doc[]", "= \""TEXT"\";"; 
	in_comment = 0;
	next;
    }
}
