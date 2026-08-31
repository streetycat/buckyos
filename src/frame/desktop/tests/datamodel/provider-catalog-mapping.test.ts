import { toKnownProviderProfile } from '../../src/api/aicc_mgr.ts'

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message)
}

const rawProvider = {
  provider_type: 'openrouter',
  provider_profile_id: 'openrouter',
  display_name: 'OpenRouter',
  protocol_adapter_id: 'openai-compatible',
  default_endpoint: 'https://openrouter.ai/api/v1',
  settings_section: 'openai',
  credential_required: true,
  metadata_drivers: ['openai', 'claude', 'google-gemini'],
}

Deno.test('provider catalog maps profile and adapter as independent identities', () => {
  const mapped = toKnownProviderProfile(rawProvider)
  assert(mapped !== null, 'known provider should map')
  assert(mapped?.provider_profile_id === 'openrouter', 'profile id should be preserved')
  assert(mapped?.protocol_adapter_id === 'openai-compatible', 'adapter id should be preserved')
  assert(mapped?.metadata_drivers.includes('claude') === true, 'model driver candidates should map')
})

Deno.test('provider catalog mapping remains linear for a large catalog', () => {
  const catalog = Array.from({ length: 10_000 }, (_, index) => ({
    ...rawProvider,
    provider_profile_id: `openrouter-${index}`,
  }))
  const started = performance.now()
  const mapped = catalog.map(toKnownProviderProfile)
  const elapsedMs = performance.now() - started
  assert(mapped.every((item) => item !== null), 'all valid providers should map')
  assert(elapsedMs < 1_000, `10k provider mappings took ${elapsedMs.toFixed(1)}ms`)
})
