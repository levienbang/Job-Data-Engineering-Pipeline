import pytest

from job_pipeline.sources.technology import is_technology_job


@pytest.mark.parametrize(
    "title,category",
    [
        ("Kỹ Sư Hệ Thống", "công_nghệ_thông_tin_kỹ_thuật_số"),
        ("Backend Developer", "tài_chính"),
        ("Senior Frontend Developer", "tài_chính"),
        ("Fullstack Engineer", "tài_chính"),
        ("Software Engineer", "xây_dựng"),
        ("Flutter Developer", "tài_chính"),
        ("Manual Tester [Dự Án Banking]", "tài_chính"),
        ("DevOps Engineer", "bán_hàng"),
        ("Cloud Engineer", "bán_hàng"),
        ("SRE", "bán_hàng"),
        ("Trưởng Nhóm An Ninh Mạng/ Cyber Security Lead", "tài_chính"),
        ("Data Engineer", "xây_dựng"),
        ("Data Scientist", "xây_dựng"),
        ("Machine Learning Engineer", "xây_dựng"),
        ("AI Developer", "xây_dựng"),
        ("Database Administrator", "tài_chính"),
    ],
)
def test_selects_technology_category_or_explicit_role(
    title: str, category: str
) -> None:
    assert is_technology_job({"job_title": title, "category": category})


@pytest.mark.parametrize(
    "title",
    [
        "Data Entry Clerk",
        "System Sales Manager",
        "Software Sales Executive",
        "QA Manager - Nhà Máy",
        "Trưởng Ban Kinh Doanh Google Cloud Platform (GCP)",
        "Chuyên Viên Tư Vấn Cloud (Cloud Presales)",
        "Thực Tập Sinh Marketing/ Digital MKT Fullstack",
        "Nhân Viên Lập Trình Phay, Tiện",
        "Kỹ Sư Lập Trình CNC",
        "Tư vấn an ninh mạng/tiền bán hàng",
    ],
)
def test_rejects_ambiguous_or_unrelated_titles(title: str) -> None:
    assert not is_technology_job(
        {"job_title": title, "category": "sản_xuất", "description": "Python data"}
    )


def test_missing_title_and_category_is_not_selected() -> None:
    assert not is_technology_job({"job_title": None, "category": None})
