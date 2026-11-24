package store

import (
	"context"
	"net/http"

	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/api"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/auth"
	"github.com/tinfoilsh/confidential-tinfoil-harness/internal/controlplane"
)

// The key registry: the caller's current content key as the control plane
// holds it, by id, and the passkey bundles that wrap it. The key itself
// never leaves the request; the control plane checks each change against
// a proof of possession.
// key 注册表：调用者当前的 content key，以 id 形式由 control plane 持有，以及包装它的 passkey bundle。
// key 本身从不离开请求；control plane 会针对持有证明（proof of possession）校验每一次变更。

// KeyState is the caller's current content key as the control plane holds
// it: its id and etag, the passkey bundles that wrap it, how it was made,
// and whether rows exist that it did not seal.
// KeyState 是 control plane 所持有的调用者当前 content key：其 id 与 etag、包装它的 passkey bundle、
// 生成方式，以及是否存在并非由它封存的 row。
type KeyState struct {
	KeyID      *string              `json:"key_id"`
	ETag       string               `json:"etag,omitempty"`
	Bundles    map[string]KeyBundle `json:"bundles"`
	CreatedVia string               `json:"created_via,omitempty"`
	CreatedAt  string               `json:"created_at,omitempty"`
	HasData    bool                 `json:"has_data"`
}

// KeyBundle is the content key wrapped under one passkey: the key AES-GCM
// sealed under the passkey's output, hex, with its IV.
// KeyBundle 是包装在某个 passkey 下的 content key：key 在 passkey 的输出下以 AES-GCM 封存，
// 连同其 IV，均为十六进制。
type KeyBundle struct {
	CredentialID  string `json:"credential_id"`
	KEKIV         string `json:"kek_iv"`
	EncryptedKeys string `json:"encrypted_keys"`
	CreatedAt     string `json:"created_at,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

// KeyBundleInput is a bundle as a client registers it.
// KeyBundleInput 是客户端注册时的 bundle。
type KeyBundleInput struct {
	CredentialID  string `json:"credential_id"`
	KEKIV         string `json:"kek_iv"`
	EncryptedKeys string `json:"encrypted_keys"`
}

const (
	// A bundle is AES-256-GCM over the 32-byte content key under a
	// 12-byte IV: the IV is 24 hex characters, and the wrapped key with
	// its tag 96.
	// bundle 是对 32 字节 content key、在 12 字节 IV 下的 AES-256-GCM：IV 为 24 个十六进制字符，
	// 带 tag 的已包装 key 为 96 个。
	bundleIVHexLen         = 24
	bundleWrappedKeyHexLen = 96
	// keyIDHexLen is a key id's length as the registry names it.
	// keyIDHexLen 是注册表所命名 key id 的长度。
	keyIDHexLen = 32
)

var errKeyNotItsID = api.Err(400, "BAD_REQUEST", "The key does not match its id")

// CurrentKey is the caller's registered key with its bundles, or the
// no-key state: no id, no bundles, and whether rows exist that predate
// the registry.
// CurrentKey 返回调用者已注册的 key 及其 bundle，或“无 key”状态：没有 id、没有 bundle，
// 以及是否存在早于注册表的 row。
func (s *Store) CurrentKey(ctx context.Context, p *auth.Principal) (*KeyState, error) {
	current, err := s.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	if err != nil {
		return nil, FromControlplane(err)
	}
	state := &KeyState{Bundles: map[string]KeyBundle{}}
	if current == nil || current.KeyID == "" {
		if current != nil {
			state.HasData = current.HasData
		}
		return state, nil
	}
	id := current.KeyID
	state.KeyID, state.ETag, state.CreatedVia, state.CreatedAt, state.HasData = &id, current.ETag, current.CreatedVia, current.CreatedAt, current.HasData
	for cid, b := range current.Bundles {
		state.Bundles[cid] = KeyBundle{CredentialID: b.CredentialID, KEKIV: b.KEKIV, EncryptedKeys: b.EncryptedKeys, CreatedAt: b.RegisteredAt, UpdatedAt: b.RegisteredAt}
	}
	return state, nil
}

// CheckRegisteredKey refuses a key that is not the caller's registered
// current key, naming the registered one when there is one: the control
// plane takes new rows only under it, so a job sealing many would be
// refused row by row.
// CheckRegisteredKey 拒绝不是调用者已注册当前 key 的 key，并在存在时指出已注册的那个：
// control plane 只接受在该 key 下的新 row，因此一次封存多份的作业会被逐 row 拒绝。
func (s *Store) CheckRegisteredKey(ctx context.Context, p *auth.Principal, key auth.ContentKey) error {
	defer clear(key[:])
	current, err := s.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	if err != nil {
		return FromControlplane(err)
	}
	if current == nil || current.KeyID != keyID(key) {
		e := api.Err(409, "KEY_MISMATCH", "The key is not the registered content key")
		if current != nil {
			e.KeyID = current.KeyID
		}
		return e
	}
	return nil
}

// CheckKeyNotReplaced refuses a key other than the one the caller has
// registered, naming it. An account with no key registered refuses none:
// its client registers the key it holds.
// CheckKeyNotReplaced 拒绝调用者已注册 key 之外的 key，并指出已注册的那个。
// 未注册任何 key 的账户不会拒绝任何 key：其客户端注册它持有的那个 key。
func (s *Store) CheckKeyNotReplaced(ctx context.Context, p *auth.Principal, key auth.ContentKey) error {
	defer clear(key[:])
	current, err := s.Controlplane.GetCurrentKey(ctx, string(p.JWT), p.ID)
	if err != nil {
		return FromControlplane(err)
	}
	if current == nil || current.KeyID == "" || current.KeyID == keyID(key) {
		return nil
	}
	e := api.Err(409, "KEY_MISMATCH", "The key is not the registered content key")
	e.KeyID = current.KeyID
	return e
}

// RegisterKey registers key as the caller's current key, with a bundle
// when given, and returns its id. ifMatch is the current key's etag, or
// "*" for an account that must have none. The control plane refuses a
// key that would strand rows another key sealed, unless the key starts
// fresh, which wipes them; the attachments it wiped leave buckets here.
// RegisterKey 将 key 注册为调用者的当前 key（给定 bundle 时一并注册），并返回其 id。
// ifMatch 是当前 key 的 etag，或对“必须没有 key”的账户用 "*"。control plane 会拒绝会
// 搁置其他 key 所封存 row 的 key，除非该 key 从零开始（start fresh），此时会清除它们；
// 被清除的 attachment 会在此处离开 buckets。
func (s *Store) RegisterKey(ctx context.Context, p *auth.Principal, key auth.ContentKey, ifMatch, createdVia, nonce string, bundle *KeyBundleInput) (string, error) {
	defer clear(key[:])
	switch createdVia {
	case "passkey", "manual", "recovery", "start_fresh":
	default:
		return "", api.Err(400, "BAD_REQUEST", "created_via must be passkey, manual, recovery or start_fresh")
	}
	if ifMatch == "" {
		return "", api.Err(400, "BAD_REQUEST", "if_match is required: the current key's etag, or \"*\" without one")
	}
	if nonce == "" {
		return "", api.Err(400, "BAD_REQUEST", "idempotency_key is required")
	}
	var initial *controlplane.RegisterKeyBundle
	if bundle != nil {
		if bundle.CredentialID == "" {
			return "", api.Err(400, "BAD_REQUEST", "initial_bundle.credential_id is required")
		}
		if err := validateBundleShape(bundle.KEKIV, bundle.EncryptedKeys); err != nil {
			return "", err
		}
		initial = &controlplane.RegisterKeyBundle{CredentialID: bundle.CredentialID, KEKIV: bundle.KEKIV, EncryptedKeys: bundle.EncryptedKeys}
	}
	kidHex := keyID(key)
	req := controlplane.RegisterKeyRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kidHex,
		IfMatch:        ifMatch,
		CreatedVia:     createdVia,
		IdempotencyKey: nonce,
		InitialBundle:  initial,
	}
	body, err := controlplane.RegisterKeyBody(req)
	if err != nil {
		return "", FromControlplane(err)
	}
	req.OperationHash = operationHash(key, http.MethodPost, controlplane.RegisterKeyPath, kidHex, ifMatch, nonce, 0, nil, body)
	// A key that replaces another, or starts fresh, fences off the search
	// index the control plane names now; which object that is must be
	// known before the fence clears the name.
	// 替换其他 key 或从零开始的 key，会隔离 control plane 现在所指明的 search 索引；
	// 必须在隔离清除该名称之前知道那是哪个对象。
	fencedObject := ""
	if s.Search != nil {
		if state, err := s.Controlplane.GetSearchIndexState(ctx, string(p.JWT), p.ID); err == nil {
			fencedObject = state.ObjectKey
		}
	}
	resp, err := s.Controlplane.RegisterKey(ctx, req)
	if err != nil {
		return "", FromControlplane(err)
	}
	deleteBucketAttachments(ctx, s.Buckets, p.ID, resp.WipedV2Attachments)
	if resp.SearchIndexFenced && s.Search != nil {
		s.Search.Forget(ctx, p.ID, fencedObject)
	}
	return kidHex, nil
}

// AddKeyBundle wraps the current key, named by keyID, under one more
// passkey.
// AddKeyBundle 将当前 key（由 keyID 命名）再包装到一个 passkey 下。
func (s *Store) AddKeyBundle(ctx context.Context, p *auth.Principal, key auth.ContentKey, kid string, bundle KeyBundleInput, nonce string) error {
	defer clear(key[:])
	if err := validateKeyID(kid); err != nil {
		return err
	}
	if bundle.CredentialID == "" || bundle.KEKIV == "" || bundle.EncryptedKeys == "" {
		return api.Err(400, "BAD_REQUEST", "credential_id, kek_iv and encrypted_keys are required")
	}
	if err := validateBundleShape(bundle.KEKIV, bundle.EncryptedKeys); err != nil {
		return err
	}
	if nonce == "" {
		return api.Err(400, "BAD_REQUEST", "idempotency_key is required")
	}
	if keyID(key) != kid {
		return errKeyNotItsID
	}
	req := controlplane.AddBundleRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kid,
		CredentialID:   bundle.CredentialID,
		KEKIV:          bundle.KEKIV,
		EncryptedKeys:  bundle.EncryptedKeys,
		IdempotencyKey: nonce,
	}
	body, err := controlplane.AddBundleBody(req)
	if err != nil {
		return FromControlplane(err)
	}
	req.OperationHash = operationHash(key, http.MethodPost, controlplane.AddBundlePath(kid), kid, "", nonce, 0, nil, body)
	if err := s.Controlplane.AddBundle(ctx, req); err != nil {
		return FromControlplane(err)
	}
	return nil
}

// RemoveKeyBundle drops one passkey's bundle from the current key, named
// by keyID.
// RemoveKeyBundle 从当前 key（由 keyID 命名）中移除某个 passkey 的 bundle。
func (s *Store) RemoveKeyBundle(ctx context.Context, p *auth.Principal, key auth.ContentKey, kid, credentialID, nonce string) error {
	defer clear(key[:])
	if err := validateKeyID(kid); err != nil {
		return err
	}
	if credentialID == "" {
		return api.Err(400, "BAD_REQUEST", "credential_id is required")
	}
	if nonce == "" {
		return api.Err(400, "BAD_REQUEST", "idempotency_key is required")
	}
	if keyID(key) != kid {
		return errKeyNotItsID
	}
	req := controlplane.RemoveBundleRequest{
		JWT:            string(p.JWT),
		ClerkUserID:    p.ID,
		KeyIDHex:       kid,
		CredentialID:   credentialID,
		IdempotencyKey: nonce,
		OperationHash:  operationHash(key, http.MethodDelete, controlplane.RemoveBundlePath(kid, credentialID), kid, "", nonce, 0, nil, nil),
	}
	if err := s.Controlplane.RemoveBundle(ctx, req); err != nil {
		return FromControlplane(err)
	}
	return nil
}

func validateKeyID(id string) error {
	if len(id) != keyIDHexLen || !isLowerHex(id) {
		return api.Err(400, "BAD_REQUEST", "key_id must be 32 lowercase hex characters")
	}
	return nil
}

// validateBundleShape holds a bundle to the one shape the registry
// stores, so no client can register a bundle another cannot open.
// validateBundleShape 将 bundle 约束为注册表存储的唯一格式，
// 从而任何客户端都不会注册出其他客户端无法打开的 bundle。
func validateBundleShape(kekIV, encryptedKeys string) error {
	if len(kekIV) != bundleIVHexLen || !isLowerHex(kekIV) {
		return api.Err(400, "BAD_REQUEST", "kek_iv must be 24 lowercase hex characters")
	}
	if len(encryptedKeys) != bundleWrappedKeyHexLen || !isLowerHex(encryptedKeys) {
		return api.Err(400, "BAD_REQUEST", "encrypted_keys must be 96 lowercase hex characters")
	}
	return nil
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
