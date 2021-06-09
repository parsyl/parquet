package dremel_test

// func TestRead(t *testing.T) {
// 	testCases := []struct {
// 		name   string
// 		f      fields.Field
// 		result string
// 	}{
// 		{
// 			name: "required and not nested",
// 			f:    fields.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"ID"}, RepetitionTypes: []fields.RepetitionType{fields.Required}},
// 			result: `func readID(x Person) int32 {
// 	return x.ID
// }`,
// 		},
// 		{
// 			name: "optional and not nested",
// 			f:    fields.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"ID"}, RepetitionTypes: []fields.RepetitionType{fields.Optional}},
// 			result: `func readID(x Person) ([]int32, []uint8, []uint8) {
// 	switch {
// 	case x.ID == nil:
// 		return nil, []uint8{0}, nil
// 	default:
// 		return []int32{*x.ID}, []uint8{1}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "required and nested",
// 			f:    fields.Field{Type: "Person", TypeName: "int32", FieldNames: []string{"Other", "Hobby", "Difficulty"}, FieldTypes: []string{"Other", "Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Required, fields.Required}},
// 			result: `func readOtherHobbyDifficulty(x Person) int32 {
// 	return x.Other.Hobby.Difficulty
// }`,
// 		},
// 		{
// 			name: "optional and nested",
// 			f:    fields.Field{Type: "Person", TypeName: "*int32", FieldNames: []string{"Hobby", "Difficulty"}, FieldTypes: []string{"Hobby", "int32"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional}},
// 			result: `func readHobbyDifficulty(x Person) ([]int32, []uint8, []uint8) {
// 	switch {
// 	case x.Hobby == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Hobby.Difficulty == nil:
// 		return nil, []uint8{1}, nil
// 	default:
// 		return []int32{*x.Hobby.Difficulty}, []uint8{2}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "mix of optional and required and nested",
// 			f:    fields.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required}},
// 			result: `func readHobbyName(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Hobby == nil:
// 		return nil, []uint8{0}, nil
// 	default:
// 		return []string{x.Hobby.Name}, []uint8{1}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "mix of optional and required and nested v2",
// 			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Hobby", "Name"}, FieldTypes: []string{"Hobby", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional}},
// 			result: `func readHobbyName(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Hobby.Name == nil:
// 		return nil, []uint8{0}, nil
// 	default:
// 		return []string{*x.Hobby.Name}, []uint8{1}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "mix of optional and require and nested 3 deep",
// 			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Required, fields.Optional}},
// 			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby.Name == nil:
// 		return nil, []uint8{1}, nil
// 	default:
// 		return []string{*x.Friend.Hobby.Name}, []uint8{2}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "mix of optional and require and nested 3 deep v2",
// 			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional}},
// 			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend.Hobby == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby.Name == nil:
// 		return nil, []uint8{1}, nil
// 	default:
// 		return []string{*x.Friend.Hobby.Name}, []uint8{2}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "mix of optional and require and nested 3 deep v3",
// 			f:    fields.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Required}},
// 			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby == nil:
// 		return nil, []uint8{1}, nil
// 	default:
// 		return []string{x.Friend.Hobby.Name}, []uint8{2}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "nested 3 deep all optional",
// 			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name"}, FieldTypes: []string{"Entity", "Item", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional}},
// 			result: `func readFriendHobbyName(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby == nil:
// 		return nil, []uint8{1}, nil
// 	case x.Friend.Hobby.Name == nil:
// 		return nil, []uint8{2}, nil
// 	default:
// 		return []string{*x.Friend.Hobby.Name}, []uint8{3}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "four deep",
// 			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Optional}},
// 			result: `func readFriendHobbyNameFirst(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby == nil:
// 		return nil, []uint8{1}, nil
// 	case x.Friend.Hobby.Name == nil:
// 		return nil, []uint8{2}, nil
// 	case x.Friend.Hobby.Name.First == nil:
// 		return nil, []uint8{3}, nil
// 	default:
// 		return []string{*x.Friend.Hobby.Name.First}, []uint8{4}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "four deep mixed",
// 			f:    fields.Field{Type: "Person", TypeName: "*string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Optional, fields.Optional, fields.Optional}},
// 			result: `func readFriendHobbyNameFirst(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend.Hobby == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby.Name == nil:
// 		return nil, []uint8{1}, nil
// 	case x.Friend.Hobby.Name.First == nil:
// 		return nil, []uint8{2}, nil
// 	default:
// 		return []string{*x.Friend.Hobby.Name.First}, []uint8{3}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "four deep mixed v2",
// 			f:    fields.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Friend", "Hobby", "Name", "First"}, FieldTypes: []string{"Entity", "Item", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Optional, fields.Optional, fields.Required}},
// 			result: `func readFriendHobbyNameFirst(x Person) ([]string, []uint8, []uint8) {
// 	switch {
// 	case x.Friend == nil:
// 		return nil, []uint8{0}, nil
// 	case x.Friend.Hobby == nil:
// 		return nil, []uint8{1}, nil
// 	case x.Friend.Hobby.Name == nil:
// 		return nil, []uint8{2}, nil
// 	default:
// 		return []string{x.Friend.Hobby.Name.First}, []uint8{3}, nil
// 	}
// }`,
// 		},
// 		{
// 			name: "repeated",
// 			f:    fields.Field{Type: "Person", TypeName: "string", FieldNames: []string{"Friends"}, FieldTypes: []string{"string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated}},
// 			result: `func readFriends(x Person) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if len(x.Friends) == 0 {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		for i0, x0 := range x.Friends {
// 			if i0 == 1 {
// 				lastRep = 1
// 			}
// 			defs = append(defs, 1)
// 			reps = append(reps, lastRep)
// 			vals = append(vals, x0)
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "readLinkFoward",
// 			f:    fields.Field{Type: "Document", TypeName: "int64", FieldNames: []string{"Link", "Forward"}, FieldTypes: []string{"Link", "int64"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated}},
// 			result: `func readLinkForward(x Document) ([]int64, []uint8, []uint8) {
// 	var vals []int64
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if x.Link == nil {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		if len(x.Link.Forward) == 0 {
// 			defs = append(defs, 1)
// 			reps = append(reps, lastRep)
// 		} else {
// 			for i0, x0 := range x.Link.Forward {
// 				if i0 == 1 {
// 					lastRep = 1
// 				}
// 				defs = append(defs, 2)
// 				reps = append(reps, lastRep)
// 				vals = append(vals, x0)
// 			}
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "readNamesLanguagesCode",
// 			f:    fields.Field{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Code"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Required}},
// 			result: `func readNamesLanguagesCode(x Document) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if len(x.Names) == 0 {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		for i0, x0 := range x.Names {
// 			if i0 == 1 {
// 				lastRep = 1
// 			}
// 			if len(x0.Languages) == 0 {
// 				defs = append(defs, 1)
// 				reps = append(reps, lastRep)
// 			} else {
// 				for i1, x1 := range x0.Languages {
// 					if i1 == 1 {
// 						lastRep = 2
// 					}
// 					defs = append(defs, 2)
// 					reps = append(reps, lastRep)
// 					vals = append(vals, x1.Code)
// 				}
// 			}
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "readNamesLanguagesCountry",
// 			f:    fields.Field{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "Languages", "Country"}, FieldTypes: []string{"Name", "Language", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Repeated, fields.Optional}},
// 			result: `func readNamesLanguagesCountry(x Document) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if len(x.Names) == 0 {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		for i0, x0 := range x.Names {
// 			if i0 == 1 {
// 				lastRep = 1
// 			}
// 			if len(x0.Languages) == 0 {
// 				defs = append(defs, 1)
// 				reps = append(reps, lastRep)
// 			} else {
// 				for i1, x1 := range x0.Languages {
// 					if i1 == 1 {
// 						lastRep = 2
// 					}
// 					if x1.Country == nil {
// 						defs = append(defs, 2)
// 						reps = append(reps, lastRep)
// 					} else {
// 						defs = append(defs, 3)
// 						reps = append(reps, lastRep)
// 						vals = append(vals, *x1.Country)
// 					}
// 				}
// 			}
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "readNamesURL",
// 			f:    fields.Field{Type: "Document", TypeName: "string", FieldNames: []string{"Names", "URL"}, FieldTypes: []string{"Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Optional}},
// 			result: `func readNamesURL(x Document) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if len(x.Names) == 0 {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		for i0, x0 := range x.Names {
// 			if i0 == 1 {
// 				lastRep = 1
// 			}
// 			if x0.URL == nil {
// 				defs = append(defs, 1)
// 				reps = append(reps, lastRep)
// 			} else {
// 				defs = append(defs, 2)
// 				reps = append(reps, lastRep)
// 				vals = append(vals, *x0.URL)
// 			}
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "run of required",
// 			f:    fields.Field{Type: "Document", TypeName: "string", FieldNames: []string{"Friends", "Name", "Last"}, FieldTypes: []string{"Friend", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Repeated, fields.Required, fields.Required}},
// 			result: `func readFriendsNameLast(x Document) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if len(x.Friends) == 0 {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		for i0, x0 := range x.Friends {
// 			if i0 == 1 {
// 				lastRep = 1
// 			}
// 			defs = append(defs, 1)
// 			reps = append(reps, lastRep)
// 			vals = append(vals, x0.Name.Last)
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "run of required v2",
// 			f:    fields.Field{Type: "Document", TypeName: "string", FieldNames: []string{"Friend", "Name", "Aliases"}, FieldTypes: []string{"Friend", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Required, fields.Required, fields.Repeated}},
// 			result: `func readFriendNameAliases(x Document) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if len(x.Friend.Name.Aliases) == 0 {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		for i0, x0 := range x.Friend.Name.Aliases {
// 			if i0 == 1 {
// 				lastRep = 1
// 			}
// 			defs = append(defs, 1)
// 			reps = append(reps, lastRep)
// 			vals = append(vals, x0)
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 		{
// 			name: "run of required v3",
// 			f:    fields.Field{Type: "Document", TypeName: "string", FieldNames: []string{"Other", "Friends", "Name", "Middle"}, FieldTypes: []string{"Other", "Friend", "Name", "string"}, RepetitionTypes: []fields.RepetitionType{fields.Optional, fields.Repeated, fields.Required, fields.Required}},
// 			result: `func readOtherFriendsNameMiddle(x Document) ([]string, []uint8, []uint8) {
// 	var vals []string
// 	var defs, reps []uint8
// 	var lastRep uint8

// 	if x.Other == nil {
// 		defs = append(defs, 0)
// 		reps = append(reps, lastRep)
// 	} else {
// 		if len(x.Other.Friends) == 0 {
// 			defs = append(defs, 1)
// 			reps = append(reps, lastRep)
// 		} else {
// 			for i0, x0 := range x.Other.Friends {
// 				if i0 == 1 {
// 					lastRep = 1
// 				}
// 				defs = append(defs, 2)
// 				reps = append(reps, lastRep)
// 				vals = append(vals, x0.Name.Middle)
// 			}
// 		}
// 	}

// 	return vals, defs, reps
// }`,
// 		},
// 	}

// 	for i, tc := range testCases {
// 		t.Run(fmt.Sprintf("%02d %s", i, tc.name), func(t *testing.T) {
// 			s := dremel.Read(tc.f)
// 			gocode, err := format.Source([]byte(s))
// 			assert.NoError(t, err)
// 			assert.Equal(t, tc.result, string(gocode))
// 		})
// 	}
// }
