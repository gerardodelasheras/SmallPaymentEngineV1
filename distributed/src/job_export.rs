//! Job-scoped accounts CSV export.
//!
//! The format matches the in-memory CLI: the fixed header and amounts rendered
//! by `Amount`'s display implementation. Rows are sorted by canonical client
//! ID so the output is reproducible.

use std::io::{self, Write};

use crate::repository::AccountRow;

pub const HEADER: &str = "client,available,held,total,locked";

pub fn write_accounts<W: Write>(out: &mut W, accounts: &[AccountRow]) -> io::Result<()> {
    writeln!(out, "{HEADER}")?;
    let mut accounts = accounts.to_vec();
    accounts.sort_by_key(|account| account.client_id);
    for account in accounts {
        writeln!(
            out,
            "{},{},{},{},{}",
            account.client_id,
            account.available,
            account.held,
            account.total(),
            account.locked
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use transaction_processor::Amount;

    use super::*;

    #[test]
    fn writes_the_fixed_header_and_sorted_rows() {
        let accounts = vec![
            AccountRow {
                client_id: 2,
                available: Amount::ZERO,
                held: Amount::ZERO,
                locked: false,
            },
            AccountRow {
                client_id: 1,
                available: Amount::parse("1.5").expect("amount"),
                held: Amount::ZERO,
                locked: false,
            },
        ];
        let mut out = Vec::new();
        write_accounts(&mut out, &accounts).expect("write");
        assert_eq!(
            String::from_utf8(out).expect("utf-8"),
            "client,available,held,total,locked\n\
             1,1.5,0.0,1.5,false\n\
             2,0.0,0.0,0.0,false\n"
        );
    }
}
