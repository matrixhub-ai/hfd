package constants

const OperationChangeKey = "operationChangeKey"

// Git 协议操作标识符 (pkg/backend/http)
const (
	OpGitGetInfoRefs     = "GIT_GET_InfoRefs"
	OpGitPostUploadPack  = "GIT_POST_UploadPack"
	OpGitPostReceivePack = "GIT_POST_ReceivePack"
)

// HuggingFace API - 认证相关 (pkg/backend/hf)
const (
	OpHfGetWhoami = "HF_GET_Whoami"
)

// HuggingFace API - 仓库管理 (pkg/backend/hf)
const (
	OpHfPostCreateRepo   = "HF_POST_CreateRepo"
	OpHfDeleteDeleteRepo = "HF_DELETE_DeleteRepo"
	OpHfPostMoveRepo     = "HF_POST_MoveRepo"
	OpHfPostValidateYaml = "HF_POST_ValidateYaml"
	OpHfPutRepoSettings  = "HF_PUT_RepoSettings"
)

// HuggingFace API - 分支操作 (pkg/backend/hf)
const (
	OpHfPostCreateBranch   = "HF_POST_CreateBranch"
	OpHfDeleteDeleteBranch = "HF_DELETE_DeleteBranch"
)

// HuggingFace API - 标签操作 (pkg/backend/hf)
const (
	OpHfPostCreateTag   = "HF_POST_CreateTag"
	OpHfDeleteDeleteTag = "HF_DELETE_DeleteTag"
)

// HuggingFace API - 仓库信息与引用 (pkg/backend/hf)
const (
	OpHfGetListRefs     = "HF_GET_ListRefs"
	OpHfGetListCommits  = "HF_GET_ListCommits"
	OpHfGetCompare      = "HF_GET_Compare"
	OpHfPostSuperSquash = "HF_POST_SuperSquash"
	OpHfGetInfoRevision = "HF_GET_InfoRevision"
	OpHfGetList         = "HF_GET_List"
)

// HuggingFace API - 文件操作 (pkg/backend/hf)
const (
	OpHfPostPreupload = "HF_POST_Preupload"
	OpHfPostCommit    = "HF_POST_Commit"
	OpHfGetTreeSize   = "HF_GET_TreeSize"
	OpHfGetTree       = "HF_GET_Tree"
	OpHfGetResolve    = "HF_GET_Resolve"
)

// LFS 对象操作 (pkg/backend/lfs)
const (
	OpLfsPostBatch        = "LFS_POST_Batch"
	OpLfsGetGetContent    = "LFS_GET_GetContent"
	OpLfsHeadGetContent   = "LFS_HEAD_GetContent"
	OpLfsPutPutContent    = "LFS_PUT_PutContent"
	OpLfsPostVerifyObject = "LFS_POST_VerifyObject"
)

// LFS 锁操作 (pkg/backend/lfs)
const (
	OpLfsGetGetLock      = "LFS_GET_GetLock"
	OpLfsPostLocksVerify = "LFS_POST_LocksVerify"
	OpLfsPostCreateLock  = "LFS_POST_CreateLock"
	OpLfsPostDeleteLock  = "LFS_POST_DeleteLock"
)
