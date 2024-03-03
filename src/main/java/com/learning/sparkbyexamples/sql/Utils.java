package com.learning.sparkbyexamples.sql;

import java.io.Serializable;

public class Utils implements Serializable {
	private static final long serialVersionUID = 5L;

	public String combineStrings(String fname, String mname, String lname) {
		return fname + "," + mname + "," + lname;
	}

}
