package main

//test 1
/*
func main() {

	tff := "Title : \"test_table\"\nTABLE_S BEGIN \n COL1 number,COL2 text\nEND DATA_SECTION :\n Data->"

	var tokens []parsers.Tff_token

	err := parsers.ParseHeader(tff, &tokens)
	if err == 1 {
		print("ERROR!")
	}

	for i, tok := range tokens {
		fmt.Printf("Token %d: Type=%v, Value=%v\n", i, tok.Token_type, tok.Token)
	}

}*/

/*
func main() {
	script := `create_table new_table(number col1, text col2 NOTNULL KEY);`

	var tokens []parsers.SC_token
	err := parsers.Parsing_script(script, &tokens)
	if err == 1 {
		fmt.Println("Parsing error!")
		return
	}

	var err_s string
	err_n := parsers.Error_checker(tokens, &err_s)
	if err_n == 1 {
		fmt.Println(err_s)
	}
		for _, t := range tokens {
			fmt.Printf("Type: %v, Token: %q\n", t.Token_type, t.Token)
		}
}
*/
/*
func main() {
	err := fileuti.WriteToFile("./hello/text.txt", "hello world!")

	if err == 1 {
		println("Fuck!")
	}
}*/
