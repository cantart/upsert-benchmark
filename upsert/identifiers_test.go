package upsert

import "testing"

func TestIsSafeIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		valid bool
	}{
		{name: "simple", ident: "users", valid: true},
		{name: "mixedCase", ident: "UserAccounts", valid: true},
		{name: "withUnderscore", ident: "user_records", valid: true},
		{name: "withDigits", ident: "user1", valid: true},
		{name: "empty", ident: "", valid: false},
		{name: "startsWithDigit", ident: "1user", valid: false},
		{name: "dash", ident: "user-name", valid: false},
		{name: "space", ident: "user name", valid: false},
		{name: "symbol", ident: "user$", valid: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSafeIdentifier(tc.ident); got != tc.valid {
				t.Fatalf("isSafeIdentifier(%q) = %v, want %v", tc.ident, got, tc.valid)
			}
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		ident string
		want  string
		err   bool
	}{
		{name: "simple", ident: "users", want: `"users"`},
		{name: "invalidStart", ident: "1user", err: true},
		{name: "disallowedChar", ident: `user"name`, err: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := quoteIdentifier(tc.ident)
			if tc.err {
				if err == nil {
					t.Fatalf("quoteIdentifier(%q) expected error, got nil", tc.ident)
				}
				return
			}
			if err != nil {
				t.Fatalf("quoteIdentifier(%q) unexpected error: %v", tc.ident, err)
			}
			if got != tc.want {
				t.Fatalf("quoteIdentifier(%q) = %q, want %q", tc.ident, got, tc.want)
			}
		})
	}
}

func TestDeriveIndexName(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		got := deriveIndexName("users", []string{"id"}, "hash_idx")
		if got != "idx_de7ebd7b26552dfc" {
			t.Fatalf("deriveIndexName simple = %q", got)
		}
	})

	t.Run("special characters", func(t *testing.T) {
		got := deriveIndexName("User Accounts", []string{"Email-Address"}, "uniq")
		if got != "idx_2f4db383e4924ea8" {
			t.Fatalf("deriveIndexName special = %q", got)
		}
	})

	t.Run("numeric table", func(t *testing.T) {
		got := deriveIndexName("123table", []string{"id"}, "hash_idx")
		if got != "idx_a61bdf0a335a4148" {
			t.Fatalf("deriveIndexName numeric = %q", got)
		}
	})
}
