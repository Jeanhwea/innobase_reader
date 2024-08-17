const IBD_01: &str = "data/departments.ibd";
const IBD_02: &str = "data/dept_manager.ibd";

#[test]
fn info_datafile() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    assert!(app
        .run(Commands::Info {
            inode_list: false,
            xdes_bitmap: false,
            all: true,
        })
        .is_ok());
}

#[test]
fn list_datafile() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    assert!(app.run(Commands::List).is_ok());
}

#[test]
fn view_fsp_hdr_page() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    assert!(app.run(Commands::View { page_no: 0 }).is_ok());
}

#[test]
fn view_inode_page() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    assert!(app.run(Commands::View { page_no: 2 }).is_ok());
}

#[test]
fn view_index_page() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    assert!(app.run(Commands::View { page_no: 4 }).is_ok());
}

#[test]
fn view_sdi_page() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    assert!(app.run(Commands::View { page_no: 3 }).is_ok());
}

#[test]
fn view_dump_data_page() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_01));
    let ans = app.run(Commands::Dump {
        page_no: 4,
        limit: 3,
        garbage: false,
        verbose: false,
    });
    assert!(ans.is_ok());
}

#[test]
fn it_works() {
    util::init_unit_test();
    let mut app = App::new(PathBuf::from(IBD_02));
    assert!(app.run(Commands::Desc).is_ok());
    assert!(app
        .run(Commands::Dump {
            page_no: 4,
            limit: 3,
            garbage: false,
            verbose: false
        })
        .is_ok());
}
