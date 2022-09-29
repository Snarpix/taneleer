use taneleer::api::{Hashsum, Sha1, Sha256, Source, SourceType, SourceTypeDiscriminants, Tag};

pub fn parse_tags(tag: &Vec<String>) -> Vec<Tag> {
    let mut tags = Vec::new();
    for t in tag {
        let mut t = t.split(':');
        let name = t.next().unwrap().to_owned();
        let value = t.next().map(|v| v.to_owned());
        tags.push(Tag { name, value });
    }
    tags
}

pub fn parse_srcs(src: &Vec<String>) -> Vec<Source> {
    let mut srcs = Vec::new();
    for s in src {
        let mut s = s.split(',');
        let name = s.next().unwrap().to_owned();
        let src_type: SourceTypeDiscriminants = s.next().unwrap().parse().unwrap();
        match src_type {
            SourceTypeDiscriminants::Url => {
                let url = s.next().unwrap().to_owned();
                let hash = s.next().unwrap().to_owned();
                let mut sha256: Sha256 = Default::default();
                hex::decode_to_slice(hash, &mut sha256).unwrap();
                let hash = Hashsum::Sha256(sha256);
                srcs.push(Source {
                    name,
                    source: SourceType::Url { url, hash },
                });
            }
            SourceTypeDiscriminants::Git => {
                let repo = s.next().unwrap().to_owned();
                let commit_str = s.next().unwrap();
                let mut commit: Sha1 = Default::default();
                hex::decode_to_slice(commit_str, &mut commit).unwrap();

                srcs.push(Source {
                    name,
                    source: SourceType::Git { repo, commit },
                });
            }
            SourceTypeDiscriminants::Artifact => {
                let uuid = uuid::Uuid::parse_str(s.next().unwrap()).unwrap();
                srcs.push(Source {
                    name,
                    source: SourceType::Artifact { uuid },
                });
            }
        }
    }
    srcs
}
