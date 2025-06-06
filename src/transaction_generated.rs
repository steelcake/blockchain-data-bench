// automatically generated by the FlatBuffers compiler, do not modify

// @generated

use core::cmp::Ordering;
use core::mem;

extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};

#[allow(unused_imports, dead_code)]
pub mod transaction {

    use core::cmp::Ordering;
    use core::mem;

    extern crate flatbuffers;
    use self::flatbuffers::{EndianScalar, Follow};

    pub enum TransactionDataOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct TransactionData<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for TransactionData<'a> {
        type Inner = TransactionData<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> TransactionData<'a> {
        pub const VT_BLOCK_HASH: flatbuffers::VOffsetT = 4;
        pub const VT_BLOCK_NUMBER: flatbuffers::VOffsetT = 6;
        pub const VT_FROM: flatbuffers::VOffsetT = 8;
        pub const VT_HASH: flatbuffers::VOffsetT = 10;
        pub const VT_INPUT: flatbuffers::VOffsetT = 12;
        pub const VT_TO: flatbuffers::VOffsetT = 14;
        pub const VT_TRANSACTION_INDEX: flatbuffers::VOffsetT = 16;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            TransactionData { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<
            'bldr: 'args,
            'args: 'mut_bldr,
            'mut_bldr,
            A: flatbuffers::Allocator + 'bldr,
        >(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            args: &'args TransactionDataArgs<'args>,
        ) -> flatbuffers::WIPOffset<TransactionData<'bldr>> {
            let mut builder = TransactionDataBuilder::new(_fbb);
            builder.add_transaction_index(args.transaction_index);
            builder.add_block_number(args.block_number);
            if let Some(x) = args.to {
                builder.add_to(x);
            }
            if let Some(x) = args.input {
                builder.add_input(x);
            }
            if let Some(x) = args.hash {
                builder.add_hash(x);
            }
            if let Some(x) = args.from {
                builder.add_from(x);
            }
            if let Some(x) = args.block_hash {
                builder.add_block_hash(x);
            }
            builder.finish()
        }

        #[inline]
        pub fn block_hash(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        TransactionData::VT_BLOCK_HASH,
                        None,
                    )
            }
        }
        #[inline]
        pub fn block_number(&self) -> u64 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<u64>(TransactionData::VT_BLOCK_NUMBER, Some(0))
                    .unwrap()
            }
        }
        #[inline]
        pub fn from(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        TransactionData::VT_FROM,
                        None,
                    )
            }
        }
        #[inline]
        pub fn hash(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        TransactionData::VT_HASH,
                        None,
                    )
            }
        }
        #[inline]
        pub fn input(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        TransactionData::VT_INPUT,
                        None,
                    )
            }
        }
        #[inline]
        pub fn to(&self) -> Option<flatbuffers::Vector<'a, u8>> {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                        TransactionData::VT_TO,
                        None,
                    )
            }
        }
        #[inline]
        pub fn transaction_index(&self) -> u64 {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab
                    .get::<u64>(TransactionData::VT_TRANSACTION_INDEX, Some(0))
                    .unwrap()
            }
        }
    }

    impl flatbuffers::Verifiable for TransactionData<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "block_hash",
                    Self::VT_BLOCK_HASH,
                    false,
                )?
                .visit_field::<u64>("block_number", Self::VT_BLOCK_NUMBER, false)?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "from",
                    Self::VT_FROM,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "hash",
                    Self::VT_HASH,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "input",
                    Self::VT_INPUT,
                    false,
                )?
                .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                    "to",
                    Self::VT_TO,
                    false,
                )?
                .visit_field::<u64>("transaction_index", Self::VT_TRANSACTION_INDEX, false)?
                .finish();
            Ok(())
        }
    }
    pub struct TransactionDataArgs<'a> {
        pub block_hash: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub block_number: u64,
        pub from: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub hash: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub input: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub to: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
        pub transaction_index: u64,
    }
    impl<'a> Default for TransactionDataArgs<'a> {
        #[inline]
        fn default() -> Self {
            TransactionDataArgs {
                block_hash: None,
                block_number: 0,
                from: None,
                hash: None,
                input: None,
                to: None,
                transaction_index: 0,
            }
        }
    }

    pub struct TransactionDataBuilder<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> TransactionDataBuilder<'a, 'b, A> {
        #[inline]
        pub fn add_block_hash(
            &mut self,
            block_hash: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                TransactionData::VT_BLOCK_HASH,
                block_hash,
            );
        }
        #[inline]
        pub fn add_block_number(&mut self, block_number: u64) {
            self.fbb_
                .push_slot::<u64>(TransactionData::VT_BLOCK_NUMBER, block_number, 0);
        }
        #[inline]
        pub fn add_from(&mut self, from: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(TransactionData::VT_FROM, from);
        }
        #[inline]
        pub fn add_hash(&mut self, hash: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(TransactionData::VT_HASH, hash);
        }
        #[inline]
        pub fn add_input(&mut self, input: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(TransactionData::VT_INPUT, input);
        }
        #[inline]
        pub fn add_to(&mut self, to: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
            self.fbb_
                .push_slot_always::<flatbuffers::WIPOffset<_>>(TransactionData::VT_TO, to);
        }
        #[inline]
        pub fn add_transaction_index(&mut self, transaction_index: u64) {
            self.fbb_
                .push_slot::<u64>(TransactionData::VT_TRANSACTION_INDEX, transaction_index, 0);
        }
        #[inline]
        pub fn new(
            _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        ) -> TransactionDataBuilder<'a, 'b, A> {
            let start = _fbb.start_table();
            TransactionDataBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<TransactionData<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for TransactionData<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("TransactionData");
            ds.field("block_hash", &self.block_hash());
            ds.field("block_number", &self.block_number());
            ds.field("from", &self.from());
            ds.field("hash", &self.hash());
            ds.field("input", &self.input());
            ds.field("to", &self.to());
            ds.field("transaction_index", &self.transaction_index());
            ds.finish()
        }
    }
    pub enum TransactionListOffset {}
    #[derive(Copy, Clone, PartialEq)]

    pub struct TransactionList<'a> {
        pub _tab: flatbuffers::Table<'a>,
    }

    impl<'a> flatbuffers::Follow<'a> for TransactionList<'a> {
        type Inner = TransactionList<'a>;
        #[inline]
        unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
            Self {
                _tab: flatbuffers::Table::new(buf, loc),
            }
        }
    }

    impl<'a> TransactionList<'a> {
        pub const VT_TRANSACTIONS: flatbuffers::VOffsetT = 4;

        #[inline]
        pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
            TransactionList { _tab: table }
        }
        #[allow(unused_mut)]
        pub fn create<
            'bldr: 'args,
            'args: 'mut_bldr,
            'mut_bldr,
            A: flatbuffers::Allocator + 'bldr,
        >(
            _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr, A>,
            args: &'args TransactionListArgs<'args>,
        ) -> flatbuffers::WIPOffset<TransactionList<'bldr>> {
            let mut builder = TransactionListBuilder::new(_fbb);
            if let Some(x) = args.transactions {
                builder.add_transactions(x);
            }
            builder.finish()
        }

        #[inline]
        pub fn transactions(
            &self,
        ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TransactionData<'a>>>>
        {
            // Safety:
            // Created from valid Table for this object
            // which contains a valid value in this slot
            unsafe {
                self._tab.get::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TransactionData>>,
                >>(TransactionList::VT_TRANSACTIONS, None)
            }
        }
    }

    impl flatbuffers::Verifiable for TransactionList<'_> {
        #[inline]
        fn run_verifier(
            v: &mut flatbuffers::Verifier,
            pos: usize,
        ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
            use self::flatbuffers::Verifiable;
            v.visit_table(pos)?
                .visit_field::<flatbuffers::ForwardsUOffset<
                    flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<TransactionData>>,
                >>("transactions", Self::VT_TRANSACTIONS, false)?
                .finish();
            Ok(())
        }
    }
    pub struct TransactionListArgs<'a> {
        pub transactions: Option<
            flatbuffers::WIPOffset<
                flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<TransactionData<'a>>>,
            >,
        >,
    }
    impl<'a> Default for TransactionListArgs<'a> {
        #[inline]
        fn default() -> Self {
            TransactionListArgs { transactions: None }
        }
    }

    pub struct TransactionListBuilder<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> {
        fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
    }
    impl<'a: 'b, 'b, A: flatbuffers::Allocator + 'a> TransactionListBuilder<'a, 'b, A> {
        #[inline]
        pub fn add_transactions(
            &mut self,
            transactions: flatbuffers::WIPOffset<
                flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<TransactionData<'b>>>,
            >,
        ) {
            self.fbb_.push_slot_always::<flatbuffers::WIPOffset<_>>(
                TransactionList::VT_TRANSACTIONS,
                transactions,
            );
        }
        #[inline]
        pub fn new(
            _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        ) -> TransactionListBuilder<'a, 'b, A> {
            let start = _fbb.start_table();
            TransactionListBuilder {
                fbb_: _fbb,
                start_: start,
            }
        }
        #[inline]
        pub fn finish(self) -> flatbuffers::WIPOffset<TransactionList<'a>> {
            let o = self.fbb_.end_table(self.start_);
            flatbuffers::WIPOffset::new(o.value())
        }
    }

    impl core::fmt::Debug for TransactionList<'_> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            let mut ds = f.debug_struct("TransactionList");
            ds.field("transactions", &self.transactions());
            ds.finish()
        }
    }
    #[inline]
    /// Verifies that a buffer of bytes contains a `TransactionList`
    /// and returns it.
    /// Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `root_as_transaction_list_unchecked`.
    pub fn root_as_transaction_list(
        buf: &[u8],
    ) -> Result<TransactionList, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root::<TransactionList>(buf)
    }
    #[inline]
    /// Verifies that a buffer of bytes contains a size prefixed
    /// `TransactionList` and returns it.
    /// Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `size_prefixed_root_as_transaction_list_unchecked`.
    pub fn size_prefixed_root_as_transaction_list(
        buf: &[u8],
    ) -> Result<TransactionList, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::size_prefixed_root::<TransactionList>(buf)
    }
    #[inline]
    /// Verifies, with the given options, that a buffer of bytes
    /// contains a `TransactionList` and returns it.
    /// Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `root_as_transaction_list_unchecked`.
    pub fn root_as_transaction_list_with_opts<'b, 'o>(
        opts: &'o flatbuffers::VerifierOptions,
        buf: &'b [u8],
    ) -> Result<TransactionList<'b>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::root_with_opts::<TransactionList<'b>>(opts, buf)
    }
    #[inline]
    /// Verifies, with the given verifier options, that a buffer of
    /// bytes contains a size prefixed `TransactionList` and returns
    /// it. Note that verification is still experimental and may not
    /// catch every error, or be maximally performant. For the
    /// previous, unchecked, behavior use
    /// `root_as_transaction_list_unchecked`.
    pub fn size_prefixed_root_as_transaction_list_with_opts<'b, 'o>(
        opts: &'o flatbuffers::VerifierOptions,
        buf: &'b [u8],
    ) -> Result<TransactionList<'b>, flatbuffers::InvalidFlatbuffer> {
        flatbuffers::size_prefixed_root_with_opts::<TransactionList<'b>>(opts, buf)
    }
    #[inline]
    /// Assumes, without verification, that a buffer of bytes contains a TransactionList and returns it.
    /// # Safety
    /// Callers must trust the given bytes do indeed contain a valid `TransactionList`.
    pub unsafe fn root_as_transaction_list_unchecked(buf: &[u8]) -> TransactionList {
        flatbuffers::root_unchecked::<TransactionList>(buf)
    }
    #[inline]
    /// Assumes, without verification, that a buffer of bytes contains a size prefixed TransactionList and returns it.
    /// # Safety
    /// Callers must trust the given bytes do indeed contain a valid size prefixed `TransactionList`.
    pub unsafe fn size_prefixed_root_as_transaction_list_unchecked(buf: &[u8]) -> TransactionList {
        flatbuffers::size_prefixed_root_unchecked::<TransactionList>(buf)
    }
    #[inline]
    pub fn finish_transaction_list_buffer<'a, 'b, A: flatbuffers::Allocator + 'a>(
        fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        root: flatbuffers::WIPOffset<TransactionList<'a>>,
    ) {
        fbb.finish(root, None);
    }

    #[inline]
    pub fn finish_size_prefixed_transaction_list_buffer<'a, 'b, A: flatbuffers::Allocator + 'a>(
        fbb: &'b mut flatbuffers::FlatBufferBuilder<'a, A>,
        root: flatbuffers::WIPOffset<TransactionList<'a>>,
    ) {
        fbb.finish_size_prefixed(root, None);
    }
} // pub mod Transaction
