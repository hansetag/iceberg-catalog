use iceberg::TableIdent;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use uuid::Uuid;

#[derive(Hash, PartialOrd, PartialEq, Debug, Clone, Copy, Eq)]
pub enum TabularIdentUuid {
    Table(Uuid),
    View(Uuid),
}

impl TabularIdentUuid {
    #[must_use]
    pub fn typ_str(&self) -> &'static str {
        match self {
            TabularIdentUuid::Table(_) => "table",
            TabularIdentUuid::View(_) => "view",
        }
    }
}

impl Display for TabularIdentUuid {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &**self)
    }
}

// We get these two types since we are using them as HashMap keys. Those need to be sized,
// implementing these types via Cow makes them not sized, so we go for two... not ideal.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TabularIdentRef<'a> {
    // TODO: TableIdent is from iceberg-rust, AFAIK, TableIdent and ViewIdent are the same, should we
    //       duplicate the type or use the same type and just accept it's called TableIdent?
    Table(&'a TableIdent),
    #[allow(dead_code)]
    View(&'a TableIdent),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TabularIdentOwned {
    // TODO: TableIdent is from iceberg-rust, AFAIK, TableIdent and ViewIdent are the same, should we
    //       duplicate the type or use the same type and just accept it's called TableIdent?
    Table(TableIdent),
    View(TableIdent),
}

impl TabularIdentOwned {
    pub(crate) fn into_inner(self) -> TableIdent {
        match self {
            TabularIdentOwned::Table(ident) | TabularIdentOwned::View(ident) => ident,
        }
    }
}

impl<'a> From<TabularIdentRef<'a>> for TabularIdentOwned {
    fn from(ident: TabularIdentRef<'a>) -> Self {
        match ident {
            TabularIdentRef::Table(ident) => TabularIdentOwned::Table(ident.clone()),
            TabularIdentRef::View(ident) => TabularIdentOwned::View(ident.clone()),
        }
    }
}

impl<'a> TabularIdentRef<'a> {
    pub(crate) fn to_table_ident_tuple(&self) -> &TableIdent {
        match self {
            TabularIdentRef::Table(ident) | TabularIdentRef::View(ident) => ident,
        }
    }
}

impl Deref for TabularIdentUuid {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        match self {
            TabularIdentUuid::Table(id) | TabularIdentUuid::View(id) => id,
        }
    }
}
